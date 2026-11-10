use std::pin::Pin;

use futures::{
    io::{AsyncBufRead, AsyncReadExt, Cursor, Take},
    stream,
};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Bytes, ReadRequest, ReaderFactory, ReaderFactoryExt,
    RecordMetadata, Result, ScanPartition, ScanPlan, ScanPlanner, async_trait, into_async_read,
};
use quick_xml::{
    NsReader, Writer, XmlVersion,
    events::{BytesStart, Event},
    name::Namespace,
};
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt};

use super::{XmlAccessor, XmlLimits, error, validation};
use crate::scan::input::ChunkInput;

type Input = Compat<Take<Pin<Box<dyn AsyncBufRead + Send>>>>;

#[async_trait]
impl<B> ScanPlanner for XmlAccessor<B>
where
    B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        let format = self.binding.factory().clone();
        // xml-arrow does not expose the inferred XmlSchema mappings for reuse.
        // Keep its bounded sample/replay reader whole rather than infer separately
        // per partition, which could change the schema and namespace mappings.
        if format.schema.is_none() {
            return Ok(ScanPlan::whole_input(self.open(request).await?));
        }
        let (_, _, schema) = format.prepare(&request)?;
        let metadata = RecordMetadata {
            schema: schema.unwrap(),
            source: Some(request.object.clone()),
        };
        let input = self
            .binding
            .storage()
            .open(ByteReadRequest::from(&request))
            .await?;
        let frames = XmlFrames::new(input, &format.record_path, format.limits.clone()).await?;
        let partitions = stream::try_unfold(frames, move |mut frames| {
            let format = format.clone();
            let request = request.clone();
            async move {
                let Some(bytes) = frames.next_document().await? else {
                    return Ok(None);
                };
                let partition: ScanPartition =
                    Box::pin(async move { format.open_reader(&ChunkInput(bytes), request).await });
                Ok(Some((partition, frames)))
            }
        });
        Ok(ScanPlan::from_stream(metadata, partitions))
    }
}

struct XmlFrames {
    reader: NsReader<Input>,
    path: Vec<String>,
    ancestors: Vec<BytesStart<'static>>,
    root_seen: bool,
    bom: u64,
    declaration: Option<Event<'static>>,
    limits: XmlLimits,
}

impl XmlFrames {
    async fn new(input: Box<dyn ByteReader>, path: &str, limits: XmlLimits) -> Result<Self> {
        let mut input = into_async_read(input, "frame_xml");
        // quick-xml needs a complete UTF-8 BOM even with single-byte sources.
        let mut prefix = Vec::new();
        (&mut input)
            .take(3)
            .read_to_end(&mut prefix)
            .await
            .map_err(error::framing)?;
        let bom = if prefix == b"\xEF\xBB\xBF" { 3 } else { 0 };
        let input: Pin<Box<dyn AsyncBufRead + Send>> = Box::pin(Cursor::new(prefix).chain(input));
        let mut reader = NsReader::from_reader(input.take(0).compat());
        reader.config_mut().check_comments = true;
        Ok(Self {
            reader,
            path: path[1..]
                .split('/')
                .map(|part| part.rsplit(':').next().unwrap().into())
                .collect(),
            ancestors: Vec::new(),
            root_seen: false,
            bom,
            declaration: None,
            limits,
        })
    }

    fn position(&self) -> u64 {
        self.reader.buffer_position() + self.bom
    }

    async fn next_document(&mut self) -> Result<Option<Bytes>> {
        let mut buffer = Vec::new();
        let mut capture: Option<Writer<Vec<u8>>> = None;
        loop {
            let before = self.position();
            let bom = if self.reader.buffer_position() == 0 {
                self.bom
            } else {
                0
            };
            let limit = (self.limits.max_record_bytes as u64).saturating_add(bom);
            let allowance = limit.saturating_add(1);
            self.reader.get_mut().get_mut().set_limit(allowance);
            buffer.clear();
            let event = self.reader.read_event_into_async(&mut buffer).await;
            if allowance - self.reader.get_mut().get_mut().limit() > limit {
                return Err(error::limit(
                    xml_arrow::Limit::EventBytes,
                    self.limits.max_record_bytes,
                    self.position(),
                ));
            }
            let event = event.map_err(error::framing)?;
            let closes = matches!(event, Event::Empty(_) | Event::End(_));
            match &event {
                Event::Start(start) | Event::Empty(start) => {
                    validation::enter(
                        self.root_seen,
                        self.ancestors.len(),
                        self.limits.max_depth,
                        self.position(),
                    )?;
                    self.root_seen = true;
                    // Preserve the source parser's validation of off-path namespace
                    // declarations and unbound element prefixes.
                    for attribute in start.attributes().with_checks(false) {
                        let attribute = attribute.map_err(error::framing)?;
                        if attribute.key.as_namespace_binding().is_some() {
                            attribute
                                .normalized_value(XmlVersion::Implicit1_0)
                                .map_err(error::framing)?;
                        }
                    }
                    let (namespace, local) = self.reader.resolver().resolve_element(start.name());
                    Option::<Namespace<'_>>::try_from(namespace).map_err(error::framing)?;
                    std::str::from_utf8(local.as_ref()).map_err(error::framing)?;
                    let on_path = self.ancestors.len() + 1 == self.path.len()
                        && self
                            .ancestors
                            .iter()
                            .chain(std::iter::once(start))
                            .zip(&self.path)
                            .all(|(element, expected)| {
                                element.local_name().as_ref() == expected.as_bytes()
                            });
                    if capture.is_none() && on_path {
                        let mut writer = Writer::new(Vec::new());
                        if let Some(declaration) = &self.declaration {
                            writer
                                .write_event(declaration.borrow())
                                .map_err(error::framing)?;
                        }
                        for ancestor in &self.ancestors {
                            writer
                                .write_event(Event::Start(ancestor.borrow()))
                                .map_err(error::framing)?;
                        }
                        capture = Some(writer);
                    }
                    self.ancestors.push(start.clone().into_owned());
                }
                Event::DocType(_) => {
                    return Err(error::invalid("DTDs and custom entities are not supported"));
                }
                Event::Decl(_) => {
                    if before != self.bom {
                        return Err(error::invalid("XML declaration must be at the beginning"));
                    }
                    self.declaration = Some(event.clone().into_owned());
                }
                Event::Eof => {
                    validation::eof(self.root_seen, self.ancestors.len())?;
                    return Ok(None);
                }
                _ => {}
            }
            if let Some(writer) = &mut capture {
                writer.write_event(event.borrow()).map_err(error::framing)?;
            } else if self.ancestors.is_empty() && !matches!(event, Event::End(_)) {
                let allowed = match &event {
                    Event::Text(text) => text
                        .as_ref()
                        .iter()
                        .all(|byte| matches!(byte, b' ' | b'\t' | b'\r' | b'\n')),
                    Event::Decl(_) | Event::Comment(_) | Event::PI(_) => true,
                    _ => false,
                };
                validation::outside_root(allowed)?;
            }
            let complete = closes && capture.is_some() && self.ancestors.len() == self.path.len();
            if closes {
                self.ancestors.pop();
            }
            if complete {
                let mut writer = capture.unwrap();
                for ancestor in self.ancestors.iter().rev() {
                    writer
                        .write_event(Event::End(ancestor.to_end()))
                        .map_err(error::framing)?;
                }
                return Ok(Some(writer.into_inner().into()));
            }
        }
    }
}

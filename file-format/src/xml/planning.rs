use std::pin::Pin;

use futures::{
    io::{AsyncBufRead, AsyncReadExt, Chain, Cursor, Take},
    stream,
};
use open_arrow_access_core::{
    Accessor, ByteMetadata, ByteReadRequest, ByteReader, Bytes, Capability, ReadRequest,
    ReaderFactory, ReaderFactoryExt, RecordMetadata, Result, ScanPartition, ScanPlan, ScanPlanner,
    StreamReader, async_trait, into_async_read,
};
use quick_xml::{
    NsReader, Writer, XmlVersion,
    events::{BytesStart, Event},
    name::Namespace,
};
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt};

use super::{XmlAccessor, XmlLimits, error, validation};

type Input = Compat<Take<Chain<Cursor<Vec<u8>>, Pin<Box<dyn AsyncBufRead + Send>>>>>;

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
                    Box::pin(async move { format.open_reader(&FrameInput(bytes), request).await });
                Ok(Some((partition, frames)))
            }
        });
        Ok(ScanPlan::from_stream(metadata, partitions))
    }
}

struct FrameInput(Bytes);

#[async_trait]
impl Accessor for FrameInput {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }
    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        Ok(Box::new(StreamReader::new(
            stream::iter([Ok(self.0.clone())]),
            ByteMetadata {
                source: Some(request.object),
                content_length: Some(self.0.len() as u64),
                version: None,
            },
        )))
    }
}

struct XmlFrames {
    reader: NsReader<Input>,
    path: Vec<String>,
    ancestors: Vec<BytesStart<'static>>,
    matches: Vec<bool>,
    ancestor_bytes: usize,
    root_seen: bool,
    bom: u64,
    declaration: Option<Event<'static>>,
    limits: XmlLimits,
}

impl XmlFrames {
    async fn new(input: Box<dyn ByteReader>, path: &str, limits: XmlLimits) -> Result<Self> {
        let mut input: Pin<Box<dyn AsyncBufRead + Send>> =
            Box::pin(into_async_read(input, "frame_xml"));
        // quick-xml needs a complete UTF-8 BOM even with single-byte sources.
        let mut prefix = Vec::new();
        (&mut input)
            .take(3)
            .read_to_end(&mut prefix)
            .await
            .map_err(error::framing)?;
        let bom = if prefix == b"\xEF\xBB\xBF" { 3 } else { 0 };
        let mut reader = NsReader::from_reader(Cursor::new(prefix).chain(input).take(0).compat());
        reader.config_mut().check_comments = true;
        Ok(Self {
            reader,
            path: path[1..]
                .split('/')
                .map(|part| part.rsplit(':').next().unwrap().into())
                .collect(),
            ancestors: Vec::new(),
            matches: Vec::new(),
            ancestor_bytes: 0,
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
                return Err(error::limit("XML event exceeds max_record_bytes"));
            }
            let event = event.map_err(error::framing)?.into_owned();
            let empty = matches!(event, Event::Empty(_));
            let closes = empty || matches!(event, Event::End(_));
            match &event {
                Event::Start(start) | Event::Empty(start) => {
                    validation::enter(self.root_seen, self.ancestors.len(), self.limits.max_depth)?;
                    if self.ancestors.is_empty() {
                        self.root_seen = true;
                    }
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
                    let local = std::str::from_utf8(local.as_ref()).map_err(error::framing)?;
                    let matches = self.matches.last().copied().unwrap_or(true)
                        && self
                            .path
                            .get(self.ancestors.len())
                            .is_some_and(|part| part == local);
                    if capture.is_none() && matches && self.ancestors.len() + 1 == self.path.len() {
                        let mut writer = Writer::new(Vec::new());
                        if let Some(declaration) = &self.declaration {
                            writer
                                .write_event(declaration.clone())
                                .map_err(error::framing)?;
                        }
                        for ancestor in &self.ancestors {
                            writer
                                .write_event(Event::Start(ancestor.clone()))
                                .map_err(error::framing)?;
                        }
                        capture = Some(writer);
                    }
                    self.ancestor_bytes = self.ancestor_bytes.saturating_add(start.len() + 2);
                    if self.ancestor_bytes > self.limits.max_record_bytes {
                        return Err(error::limit(
                            "XML ancestor envelope exceeds max_record_bytes",
                        ));
                    }
                    self.ancestors.push(start.clone());
                    self.matches.push(matches);
                }
                Event::DocType(_) => {
                    return Err(error::invalid("DTDs and custom entities are not supported"));
                }
                Event::Decl(_) => {
                    if before != self.bom {
                        return Err(error::invalid("XML declaration must be at the beginning"));
                    }
                    self.declaration = Some(event.clone());
                }
                Event::Eof => {
                    validation::eof(self.root_seen, self.ancestors.len())?;
                    return Ok(None);
                }
                _ => {}
            }
            if let Some(writer) = &mut capture {
                writer.write_event(event.clone()).map_err(error::framing)?;
                if writer.get_ref().len() > self.limits.max_batch_bytes {
                    return Err(error::limit("framed XML document exceeds max_batch_bytes"));
                }
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
                if let Some(ancestor) = self.ancestors.pop() {
                    self.ancestor_bytes -= ancestor.len() + 2;
                }
                self.matches.pop();
            }
            if complete {
                let mut writer = capture.unwrap();
                for ancestor in self.ancestors.iter().rev() {
                    writer
                        .write_event(Event::End(ancestor.to_end()))
                        .map_err(error::framing)?;
                }
                if writer.get_ref().len() > self.limits.max_batch_bytes {
                    return Err(error::limit("framed XML document exceeds max_batch_bytes"));
                }
                return Ok(Some(writer.into_inner().into()));
            }
        }
    }
}

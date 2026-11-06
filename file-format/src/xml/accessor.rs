use futures::{StreamExt, TryStreamExt};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, Error, ErrorKind, ReadRequest,
    RecordMetadata, RecordReader, Result, async_trait, into_stream,
};
use xml_arrow::{ReaderOptions, XmlReader, XmlSchema};

use super::{reader::XmlRecordReader, xml_error};

/// Limits on XML parsing. Byte limits measure serialized XML, not Arrow allocations.
/// One lookahead record and the upstream byte chunk may also be retained.
#[derive(Clone, Debug)]
pub struct XmlLimits {
    /// Maximum serialized record or input event, including inherited namespaces.
    pub max_record_bytes: usize,
    /// Maximum serialized record bytes per batch; must cover one maximum record.
    pub max_batch_bytes: usize,
    /// Maximum document element depth, including unselected content.
    pub max_depth: usize,
}

impl Default for XmlLimits {
    fn default() -> Self {
        let options = ReaderOptions::default();
        Self {
            max_record_bytes: options.max_record_bytes,
            max_batch_bytes: options.max_batch_bytes,
            max_depth: options.max_depth,
        }
    }
}

/// Select XML records by an absolute element path and explicit field mappings.
/// Namespace prefixes resolve through the schema's URI bindings. External
/// compression composes through the byte accessor. No file/partition parallelism.
#[derive(Clone, Debug)]
pub struct XmlAccessor<A> {
    inner: A,
    schema: XmlSchema,
    record_path: String,
    limits: XmlLimits,
}

impl<A> XmlAccessor<A> {
    /// Configure explicit XML mappings and record selection. Options are
    /// validated by `open` before the byte accessor is called.
    pub fn new(inner: A, schema: XmlSchema, record_path: impl Into<String>) -> Self {
        Self {
            inner,
            schema,
            record_path: record_path.into(),
            limits: XmlLimits::default(),
        }
    }

    /// Override XML limits. Invalid limits fail at open, before input I/O.
    pub fn with_limits(mut self, limits: XmlLimits) -> Self {
        self.limits = limits;
        self
    }

    fn options(&self) -> ReaderOptions {
        ReaderOptions {
            max_record_bytes: self.limits.max_record_bytes,
            max_batch_bytes: self.limits.max_batch_bytes,
            max_depth: self.limits.max_depth,
            ..ReaderOptions::default()
        }
    }
}

#[async_trait]
impl<A> Accessor for XmlAccessor<A>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    type Request = ReadRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> Capability {
        Capability {
            read: self.inner.capability().read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        let (schema, projection) = request
            .options
            .resolve_projection(&self.schema.arrow_schema())?;
        let options = ReaderOptions {
            batch_size: request.options.batch_size.get(),
            projection,
            ..self.options()
        };
        options
            .validate(&self.schema, &self.record_path)
            .map_err(xml_error)?;
        let input = self
            .inner
            .open(ByteReadRequest::new(request.object.clone()).with_options(request.byte_options))
            .await?;
        let input = into_stream(input)
            .map(|result| {
                result
                    .and_then(|bytes| {
                        if bytes.is_empty() {
                            Err(Error::new(
                                ErrorKind::Io,
                                "read_xml",
                                "byte reader returned an empty chunk",
                            ))
                        } else {
                            Ok(bytes)
                        }
                    })
                    .map_err(std::io::Error::other)
            })
            .into_async_read();
        let reader = XmlReader::new(input, self.schema.clone(), &self.record_path, options)
            .map_err(xml_error)?;
        Ok(Box::new(XmlRecordReader {
            reader,
            metadata: RecordMetadata {
                schema,
                source: Some(request.object),
            },
        }))
    }
}

use futures::{StreamExt, TryStreamExt};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, Error, ErrorKind, ReadRequest, ReadState,
    RecordMetadata, RecordReader, Result, async_trait, into_stream,
};
use xml_arrow::{InferenceOptions, NamespaceBinding, ReaderOptions, XmlReader, XmlSchema};

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

/// Select XML records by an absolute element path and infer a fixed output schema.
/// Explicit field mappings are optional. External
/// compression composes through the byte accessor. No file/partition parallelism.
#[derive(Clone, Debug)]
pub struct XmlAccessor<A> {
    inner: A,
    schema: Option<XmlSchema>,
    record_path: String,
    limits: XmlLimits,
    namespaces: Vec<(String, String)>,
    inference: InferenceOptions,
}

impl<A> XmlAccessor<A> {
    /// Select records. `open` infers a fixed schema from a bounded sample and
    /// replays sampled records before continuing with the remaining input.
    pub fn new(inner: A, record_path: impl Into<String>) -> Self {
        Self {
            inner,
            schema: None,
            record_path: record_path.into(),
            limits: XmlLimits::default(),
            namespaces: Vec::new(),
            inference: InferenceOptions::default(),
        }
    }

    /// Use explicit field mappings instead of inferring a schema.
    /// Record-path prefixes resolve through this schema's bindings.
    pub fn with_schema(mut self, schema: XmlSchema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Bind record-path prefixes for schema inference. Explicit schemas carry
    /// their own bindings and do not use this setting.
    pub fn with_namespaces(mut self, bindings: &[NamespaceBinding<'_>]) -> Self {
        self.namespaces = bindings
            .iter()
            .map(|binding| (binding.prefix.to_owned(), binding.uri.to_owned()))
            .collect();
        self
    }

    /// Bound inference sampling. Defaults to 128 records and 8 MiB of XML.
    /// Only used when no explicit schema is supplied.
    pub fn with_inference_options(mut self, options: InferenceOptions) -> Self {
        self.inference = options;
        self
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
        let bindings: Vec<_> = self
            .namespaces
            .iter()
            .map(|(prefix, uri)| NamespaceBinding { prefix, uri })
            .collect();
        let validation_schema = match &self.schema {
            Some(schema) => schema.clone(),
            None => {
                if self.inference.max_records == 0 || self.inference.max_bytes == 0 {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "read_xml",
                        "inference record and byte limits must be positive",
                    ));
                }
                XmlSchema::with_namespaces(vec![], &bindings).map_err(|error| {
                    Error::new(ErrorKind::InvalidInput, "read_xml", error.to_string())
                        .with_source(error)
                })?
            }
        };
        // Explicit mappings allow projection validation before storage I/O.
        // Inference resolves column names after sampling the input.
        let explicit_projection = self
            .schema
            .as_ref()
            .map(|schema| request.options.resolve_projection(&schema.arrow_schema()))
            .transpose()?;
        let options = ReaderOptions {
            batch_size: request.options.batch_size.get(),
            projection: explicit_projection
                .as_ref()
                .and_then(|(_, projection)| projection.clone()),
            ..self.options()
        };
        options
            .validate(&validation_schema, &self.record_path)
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
        let (reader, schema, projection) = if let Some((schema, _)) = explicit_projection {
            let reader = XmlReader::new(input, validation_schema, &self.record_path, options)
                .map_err(xml_error)?;
            (reader, schema, None)
        } else {
            let reader = XmlReader::infer(
                input,
                &self.record_path,
                &bindings,
                options,
                self.inference.clone(),
            )
            .await
            .map_err(xml_error)?;
            let (schema, projection) = request.options.resolve_projection(&reader.schema())?;
            (reader, schema, projection)
        };
        Ok(Box::new(XmlRecordReader {
            reader,
            projection,
            state: ReadState::default(),
            metadata: RecordMetadata {
                schema,
                source: Some(request.object),
            },
        }))
    }
}

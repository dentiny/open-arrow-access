use crate::{
    inference::{ResolvedFormat, SchemaInference},
    preparation::{PreparedProjection, prepare_projection},
};

use super::{
    CsvDialect, CsvFormat, CsvHeader,
    inference::{self, CsvInferenceOptions},
    reader::csv_reader,
    validation,
};
use arrow_csv::{ReaderBuilder, reader::Decoder};
use arrow_schema::{DataType, Schema};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, FactoryToken, OrderRequirement, ReadRequest,
    ReaderFactory, RecordCapability, RecordMetadata, RecordReader, Result, SchemaRef, async_trait,
};
use open_arrow_access_decompression::CompressionFactory;
use std::sync::Arc;

/// Incrementally read CSV or TSV using an explicit or inferred positional schema.
/// Selects comma for `.csv` and tab for `.tsv` per input, with automatic header
/// detection. Other extensions use the configured delimiter (comma by default). Use
/// [`Self::with_dialect`] for structural options and [`Self::with_header`] to override.
/// Extension matching is case-insensitive and strips one suffix registered with
/// the compression factory; configure byte decompression separately.
/// Recognized extensions override the configured delimiter, preserving all other
/// dialect options. Locators are opaque: query/fragment characters are not stripped.
#[derive(Clone, Debug)]
pub struct CsvFileFormat {
    schema: Option<SchemaRef>,
    inference: CsvInferenceOptions,
    options: CsvOptions,
    header: CsvHeader,
    delimiter_resolved: bool,
    compression_factory: CompressionFactory,
}

#[derive(Clone, Debug)]
enum CsvOptions {
    Dialect(CsvDialect),
    Custom(CsvFormat),
}

/// Validated CSV decoders and output schema for one open request.
pub struct PreparedCsvReader {
    pub schema: SchemaRef,
    pub decoder: Decoder,
    pub header: Option<Decoder>,
}

impl CsvFileFormat {
    /// Infer a fixed schema per input, with automatic header detection.
    pub fn infer() -> Self {
        Self {
            schema: None,
            inference: CsvInferenceOptions::default(),
            options: CsvOptions::Dialect(CsvDialect::default()),
            header: CsvHeader::Auto,
            delimiter_resolved: false,
            compression_factory: CompressionFactory::default(),
        }
    }

    pub(super) fn for_request(&self, request: &ReadRequest) -> Self {
        let mut format = self.clone();
        // Resolve once, before inference or framing. Workers consume canonical
        // comma-separated bytes even when the original locator ends in `.tsv`.
        format.delimiter_resolved = true;
        if !self.delimiter_resolved
            && let Some(delimiter) = super::dialect::delimiter_from_path(
                self.compression_factory
                    .strip_suffix(request.object.locator()),
            )
        {
            match &mut format.options {
                CsvOptions::Dialect(dialect) => dialect.delimiter = delimiter,
                CsvOptions::Custom(options) => {
                    *options = options.clone().with_delimiter(delimiter);
                }
            }
        }
        format
    }

    /// Share compression suffix registrations with the byte decompression layer.
    /// This affects suffix recognition only; bytes are still decoded by the layer.
    pub fn with_compression_factory(mut self, factory: CompressionFactory) -> Self {
        self.compression_factory = factory;
        self
    }

    pub fn with_inference_options(mut self, options: CsvInferenceOptions) -> Self {
        self.inference = options;
        self
    }

    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema: Some(schema),
            ..Self::infer()
        }
    }

    pub(super) fn parallel_dialect(&self) -> Option<CsvDialect> {
        match &self.options {
            CsvOptions::Dialect(dialect) => Some(CsvDialect {
                header: self.header,
                ..*dialect
            }),
            CsvOptions::Custom(_) => None,
        }
    }

    pub fn with_format(mut self, format: CsvFormat) -> Self {
        self.header = format.has_header.into();
        self.options = CsvOptions::Custom(format);
        self
    }
    /// Configure a known CSV dialect for both sequential decoding and parallel framing.
    /// The default dialect keeps automatic header detection in both schema modes.
    /// Opaque Arrow CsvFormat settings remain supported for explicit schemas through
    /// with_format, using a whole-input plan because their settings cannot be inspected.
    pub fn with_dialect(mut self, dialect: CsvDialect) -> Self {
        self.header = dialect.header;
        self.options = CsvOptions::Dialect(dialect);
        self
    }

    /// Override the header policy. Auto requires an inspectable CsvDialect;
    /// opaque with_format settings support only Present and Absent.
    pub fn with_header(mut self, header: CsvHeader) -> Self {
        self.header = header;
        self
    }

    pub(super) fn prepare_scan(&self, request: &ReadRequest) -> Result<Option<PreparedProjection>> {
        if self.schema.is_none() || self.header == CsvHeader::Auto {
            validation::require_dialect(self.parallel_dialect())?;
        }
        let Some(input_schema) = &self.schema else {
            self.inference.validate()?;
            return Ok(None);
        };
        let options = &request.options;
        validation::validate_input_schema(input_schema)?;
        let prepared = prepare_projection(input_schema, options)?;
        validation::validate_selected_schema(&prepared.schema)?;
        Ok(Some(prepared))
    }

    fn decoder(&self, prepared: PreparedProjection, request: &ReadRequest) -> PreparedCsvReader {
        let schema = self.schema.as_ref().expect("resolved CSV schema");
        let format = match &self.options {
            CsvOptions::Dialect(dialect) => dialect.format(),
            CsvOptions::Custom(format) => format.clone(),
        }
        .with_header(self.header == CsvHeader::Present);
        let validate_header = format.has_header && format.validate_header;
        let header = validate_header.then(|| {
            let fields = schema
                .fields()
                .iter()
                .map(|field| {
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(DataType::Utf8)
                        .with_nullable(true)
                })
                .collect::<Vec<_>>();
            ReaderBuilder::new(Arc::new(Schema::new(fields)))
                .with_format(format.inner.clone().with_header(false).with_null_regex(
                    regex::Regex::new(r"\b\B").expect("valid never-matching regex"),
                ))
                .with_batch_size(1)
                .build_decoder()
        });
        let format = format.inner;
        let mut builder = ReaderBuilder::new(schema.clone())
            .with_format(format)
            .with_batch_size(request.options.batch_size.get());
        if let Some(indices) = prepared.indices {
            builder = builder.with_projection(indices);
        }
        PreparedCsvReader {
            schema: prepared.schema,
            decoder: builder.build_decoder(),
            header,
        }
    }
}

#[async_trait]
impl SchemaInference for CsvFileFormat {
    async fn resolve_schema(
        &self,
        input: Box<dyn ByteReader>,
    ) -> Result<Option<ResolvedFormat<Self>>> {
        if self.schema.is_some() && self.header != CsvHeader::Auto {
            return Ok(Some(ResolvedFormat {
                format: self.clone(),
                input,
            }));
        }
        let dialect = validation::require_dialect(self.parallel_dialect())?;
        let Some(inference::ResolvedCsvInput {
            schema,
            has_header,
            input,
        }) = inference::resolve(input, dialect, self.inference, self.schema.clone()).await?
        else {
            return Ok(None);
        };
        let mut format = Self::new(schema).with_dialect(CsvDialect {
            header: has_header.into(),
            validate_header: dialect.validate_header,
            allow_truncated_rows: dialect.allow_truncated_rows,
            ..CsvDialect::default()
        });
        // Inference replays canonical CSV, regardless of the source extension.
        format.delimiter_resolved = true;
        Ok(Some(ResolvedFormat { format, input }))
    }
}

#[async_trait]
impl ReaderFactory for CsvFileFormat {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    type Prepared = Option<PreparedCsvReader>;

    fn capability(&self, input: Capability) -> RecordCapability {
        Capability {
            read: input.read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
        .with_ordering(OrderRequirement::Reorderable)
    }

    fn prepare(&self, request: &ReadRequest) -> Result<Self::Prepared> {
        let format = self.for_request(request);
        let prepared = format.prepare_scan(request)?;
        if format.header == CsvHeader::Auto {
            return Ok(None);
        }
        Ok(prepared.map(|prepared| format.decoder(prepared, request)))
    }

    async fn metadata(
        &self,
        storage: &dyn Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
        request: ReadRequest,
    ) -> Result<RecordMetadata> {
        let format = self.for_request(&request);
        format.prepare_scan(&request)?;
        let format = if format.schema.is_some() && format.header != CsvHeader::Auto {
            format
        } else {
            let input = storage.open(ByteReadRequest::from(&request)).await?;
            let Some(resolved) = format.resolve_schema(input).await? else {
                return Ok(crate::inference::empty_plan(request).metadata);
            };
            resolved.format
        };
        let schema = format
            .prepare_scan(&request)?
            .expect("resolved CSV schema")
            .schema;
        Ok(RecordMetadata {
            schema,
            source: Some(request.object),
        })
    }

    async fn create_reader(
        &self,
        _token: &FactoryToken,
        input: Box<dyn ByteReader>,
        request: ReadRequest,
        prepared: Self::Prepared,
    ) -> Result<Box<dyn RecordReader>> {
        let (input, schema, decoder, header) = match prepared {
            Some(PreparedCsvReader {
                schema,
                decoder,
                header,
            }) => (input, schema, decoder, header),
            None => {
                let format = self.for_request(&request);
                let Some(ResolvedFormat { format, input }) = format.resolve_schema(input).await?
                else {
                    return Ok(Box::new(
                        crate::inference::empty_plan(request).into_reader(),
                    ));
                };
                let PreparedCsvReader {
                    schema,
                    decoder,
                    header,
                } = format.prepare(&request)?.expect("resolved CSV schema");
                (input, schema, decoder, header)
            }
        };
        Ok(Box::new(csv_reader(
            input,
            decoder,
            header,
            RecordMetadata {
                schema,
                source: Some(request.object),
            },
        )))
    }
}

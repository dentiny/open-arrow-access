use crate::{inference::SchemaInference, preparation::PreparedProjection};

use super::{
    CsvDialect, CsvFormat, CsvHeader,
    inference::{self, CsvInferenceOptions},
    reader::csv_reader,
    validation,
};
use arrow_csv::{ReaderBuilder, reader::Decoder};
use arrow_schema::{DataType, Schema};
use open_arrow_access_core::{
    ByteReader, Capability, FactoryToken, OrderRequirement, ReadRequest, ReaderFactory,
    RecordCapability, RecordMetadata, RecordReader, Result, SchemaRef, async_trait,
};
use std::sync::Arc;

/// Incrementally read CSV or TSV using an explicit or inferred positional schema.
/// Defaults to comma-separated input with automatic header detection. Use
/// [`Self::with_dialect`] for structural options and [`Self::with_header`] to override.
#[derive(Clone, Debug)]
pub struct CsvFileFormat {
    schema: Option<SchemaRef>,
    inference: CsvInferenceOptions,
    options: CsvOptions,
    header: CsvHeader,
}

#[derive(Clone, Debug)]
enum CsvOptions {
    Dialect(CsvDialect),
    Custom(CsvFormat),
}

impl CsvFileFormat {
    /// Infer a fixed schema per input, with automatic header detection.
    pub fn infer() -> Self {
        Self {
            schema: None,
            inference: CsvInferenceOptions::default(),
            options: CsvOptions::Dialect(CsvDialect::default()),
            header: CsvHeader::Auto,
        }
    }

    pub fn with_inference_options(mut self, options: CsvInferenceOptions) -> Self {
        self.inference = options;
        self
    }

    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema: Some(schema),
            inference: CsvInferenceOptions::default(),
            options: CsvOptions::Dialect(CsvDialect::default()),
            header: CsvHeader::Auto,
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
        if self.header == CsvHeader::Auto {
            validation::require_dialect(self.parallel_dialect())?;
        }
        let Some(input_schema) = &self.schema else {
            self.inference.validate()?;
            validation::require_dialect(self.parallel_dialect())?;
            return Ok(None);
        };
        let options = &request.options;
        validation::validate_input_schema(input_schema)?;
        let prepared = PreparedProjection::new(input_schema, options)?;
        validation::validate_selected_schema(&prepared.schema)?;
        Ok(Some(prepared))
    }

    fn decoder(
        &self,
        prepared: PreparedProjection,
        request: &ReadRequest,
    ) -> (SchemaRef, Decoder, Option<Decoder>) {
        let schema = self.schema.as_ref().expect("resolved CSV schema");
        let format = match &self.options {
            CsvOptions::Dialect(dialect) => CsvDialect {
                header: self.header,
                ..*dialect
            }
            .format(),
            CsvOptions::Custom(format) => format
                .clone()
                .with_header(self.header == CsvHeader::Present),
        };
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
        (prepared.schema, builder.build_decoder(), header)
    }
}

#[async_trait]
impl SchemaInference for CsvFileFormat {
    async fn resolve_schema(
        &self,
        input: Box<dyn ByteReader>,
    ) -> Result<(Self, Box<dyn ByteReader>)> {
        if self.schema.is_some() && self.header != CsvHeader::Auto {
            return Ok((self.clone(), input));
        }
        let dialect = validation::require_dialect(self.parallel_dialect())?;
        let (schema, has_header, input) =
            inference::resolve(input, dialect, self.inference, self.schema.clone()).await?;
        let format = Self::new(schema).with_dialect(CsvDialect {
            header: has_header.into(),
            validate_header: dialect.validate_header,
            allow_truncated_rows: dialect.allow_truncated_rows,
            ..CsvDialect::default()
        });
        Ok((format, input))
    }
}

#[async_trait]
impl ReaderFactory for CsvFileFormat {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    type Prepared = Option<(SchemaRef, Decoder, Option<Decoder>)>;

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
        let prepared = self.prepare_scan(request)?;
        if self.header == CsvHeader::Auto {
            return Ok(None);
        }
        Ok(prepared.map(|prepared| self.decoder(prepared, request)))
    }

    async fn create_reader(
        &self,
        _token: &FactoryToken,
        input: Box<dyn ByteReader>,
        request: ReadRequest,
        prepared: Self::Prepared,
    ) -> Result<Box<dyn RecordReader>> {
        let (input, schema, decoder, header) = match prepared {
            Some((schema, decoder, header)) => (input, schema, decoder, header),
            None => {
                let (format, input) = self.resolve_schema(input).await?;
                let (schema, decoder, header) =
                    format.prepare(&request)?.expect("resolved CSV schema");
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

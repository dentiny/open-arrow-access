use crate::inference::SchemaInference;

use super::{
    CsvDialect, CsvFormat,
    inference::{self, CsvInferenceOptions},
    reader::csv_reader,
    validation,
};
use arrow_csv::{ReaderBuilder, reader::Decoder};
use open_arrow_access_core::{
    ByteReader, Capability, Error, ErrorKind, FactoryToken, OrderRequirement, ReadRequest,
    ReaderFactory, RecordCapability, RecordMetadata, RecordReader, Result, SchemaRef, async_trait,
};

/// Incrementally read CSV or TSV using an explicit or inferred positional schema.
/// Defaults to comma-separated input without a header; use [`Self::with_format`]
/// to select a tab delimiter, header handling and other Arrow CSV options.
#[derive(Clone, Debug)]
pub struct CsvFileFormat {
    schema: Option<SchemaRef>,
    inference: CsvInferenceOptions,
    options: CsvOptions,
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
        }
    }

    pub(super) fn parallel_dialect(&self) -> Option<CsvDialect> {
        match &self.options {
            CsvOptions::Dialect(dialect) => Some(*dialect),
            CsvOptions::Custom(_) => None,
        }
    }

    pub fn with_format(mut self, format: CsvFormat) -> Self {
        self.options = CsvOptions::Custom(format);
        self
    }
    /// Configure a known CSV dialect for both sequential decoding and parallel framing.
    /// With inference, this also explicitly sets header presence from `has_header`.
    /// Install inference options afterwards to restore automatic detection.
    /// Opaque Arrow CsvFormat settings remain supported for explicit schemas through
    /// with_format, using a whole-input plan because their settings cannot be inspected.
    pub fn with_dialect(mut self, dialect: CsvDialect) -> Self {
        if self.schema.is_none() {
            self.inference.has_header = Some(dialect.has_header);
        }
        self.options = CsvOptions::Dialect(dialect);
        self
    }
}

#[async_trait]
impl SchemaInference for CsvFileFormat {
    async fn resolve_schema(
        &self,
        input: Box<dyn ByteReader>,
    ) -> Result<(Self, Box<dyn ByteReader>)> {
        if self.schema.is_some() {
            return Ok((self.clone(), input));
        }
        let dialect = self.parallel_dialect().ok_or_else(|| Error::new(ErrorKind::InvalidInput, "infer_csv", "schema inference requires an inspectable CsvDialect; use with_dialect instead of with_format"))?;
        let (schema, has_header, input) = inference::infer(input, dialect, self.inference).await?;
        let format = Self::new(schema).with_dialect(CsvDialect {
            has_header,
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
    type Prepared = Option<(SchemaRef, Decoder)>;

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
        let Some(input_schema) = &self.schema else {
            self.inference.validate()?;
            if self.parallel_dialect().is_none() {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "infer_csv",
                    "schema inference requires an inspectable CsvDialect; use with_dialect instead of with_format",
                ));
            }
            return Ok(None);
        };
        let options = &request.options;
        validation::validate_input_schema(input_schema)?;
        let (schema, projection) = options.resolve_projection(input_schema)?;
        validation::validate_selected_schema(&schema)?;
        let mut builder = ReaderBuilder::new(input_schema.clone())
            .with_format(match &self.options {
                CsvOptions::Dialect(dialect) => dialect.format(),
                CsvOptions::Custom(format) => format.clone(),
            })
            .with_batch_size(options.batch_size.get());
        if let Some(indices) = projection {
            builder = builder.with_projection(indices);
        }
        Ok(Some((schema, builder.build_decoder())))
    }

    async fn create_reader(
        &self,
        _token: &FactoryToken,
        input: Box<dyn ByteReader>,
        request: ReadRequest,
        prepared: Self::Prepared,
    ) -> Result<Box<dyn RecordReader>> {
        let (input, schema, decoder) = match prepared {
            Some((schema, decoder)) => (input, schema, decoder),
            None => {
                let (format, input) = self.resolve_schema(input).await?;
                let (schema, decoder) = format.prepare(&request)?.expect("resolved CSV schema");
                (input, schema, decoder)
            }
        };
        Ok(Box::new(csv_reader(
            input,
            decoder,
            RecordMetadata {
                schema,
                source: Some(request.object),
            },
        )))
    }
}

use crate::{inference::SchemaInference, preparation::PreparedProjection};

use super::{
    CsvDialect, CsvFormat,
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
/// Defaults to comma-separated input without a header; use [`Self::with_format`]
/// to select a tab delimiter, header handling and other Arrow CSV options.
#[derive(Clone, Debug)]
pub struct CsvFileFormat {
    schema: Option<SchemaRef>,
    all_varchar: bool,
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
            all_varchar: false,
            inference: CsvInferenceOptions::default(),
            options: CsvOptions::Dialect(CsvDialect::default()),
        }
    }

    /// Use the first CSV record as column names and read every column as nullable
    /// `Utf8`. Header-only and all-null columns are supported. The input is opened
    /// once; records following the header remain available to the reader. Empty
    /// input still requires an explicit schema because it has no column names.
    ///
    /// Configure TSV and other known dialects with [`Self::with_dialect`], keeping
    /// `has_header: true`. Header policy can also be overridden with inference options.
    pub fn from_header() -> Self {
        Self::infer().all_varchar().with_dialect(CsvDialect {
            has_header: true,
            ..Default::default()
        })
    }

    /// Read every column as nullable `Utf8`, preserving field names and metadata.
    ///
    /// Inferred schemas retain the configured header detection policy. With an
    /// explicit header policy, only the first record is needed to establish the
    /// schema. Explicit schemas have their field types and nullability replaced.
    /// Delimiter, quoting, null handling and row-width validation are unchanged.
    pub fn all_varchar(mut self) -> Self {
        self.all_varchar = true;
        self.schema = self
            .schema
            .map(|schema| Arc::new(inference::varchar_schema(&schema)));
        self
    }

    pub fn with_inference_options(mut self, options: CsvInferenceOptions) -> Self {
        self.inference = options;
        self
    }

    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema: Some(schema),
            all_varchar: false,
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

    pub(super) fn prepare_scan(&self, request: &ReadRequest) -> Result<Option<PreparedProjection>> {
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
            CsvOptions::Dialect(dialect) => dialect.format(),
            CsvOptions::Custom(format) => format.clone(),
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
        if self.schema.is_some() {
            return Ok((self.clone(), input));
        }
        let dialect = validation::require_dialect(self.parallel_dialect())?;
        let (schema, has_header, input) =
            inference::infer(input, dialect, self.inference, self.all_varchar).await?;
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
        Ok(self
            .prepare_scan(request)?
            .map(|prepared| self.decoder(prepared, request)))
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

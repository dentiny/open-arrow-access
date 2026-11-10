use super::{CsvDialect, CsvFormat, reader::csv_reader, validation};
use arrow_csv::{ReaderBuilder, reader::Decoder};
use open_arrow_access_core::{
    ByteReader, FactoryToken, ReadRequest, ReaderFactory, RecordMetadata, RecordReader, Result,
    SchemaRef, async_trait,
};

/// Incrementally read CSV or TSV using an explicit positional schema.
/// Defaults to comma-separated input without a header; use [`Self::with_format`]
/// to select a tab delimiter, header handling and other Arrow CSV options.
#[derive(Clone, Debug)]
pub struct CsvFileFormat {
    schema: SchemaRef,
    options: CsvOptions,
}

#[derive(Clone, Debug)]
enum CsvOptions {
    Dialect(CsvDialect),
    Custom(CsvFormat),
}

impl CsvFileFormat {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
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
    /// Opaque Arrow CsvFormat settings remain supported through with_format, using
    /// a whole-input plan because their private framing options cannot be inspected.
    pub fn with_dialect(mut self, dialect: CsvDialect) -> Self {
        self.options = CsvOptions::Dialect(dialect);
        self
    }
}

#[async_trait]
impl ReaderFactory for CsvFileFormat {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    type Prepared = (SchemaRef, Decoder);

    fn prepare(&self, request: &ReadRequest) -> Result<Self::Prepared> {
        let options = &request.options;
        validation::validate_input_schema(&self.schema)?;
        let (schema, projection) = options.resolve_projection(&self.schema)?;
        validation::validate_selected_schema(&schema)?;
        let mut builder = ReaderBuilder::new(self.schema.clone())
            .with_format(match &self.options {
                CsvOptions::Dialect(dialect) => dialect.format(),
                CsvOptions::Custom(format) => format.clone(),
            })
            .with_batch_size(options.batch_size.get());
        if let Some(indices) = projection {
            builder = builder.with_projection(indices);
        }
        Ok((schema, builder.build_decoder()))
    }

    async fn create_reader(
        &self,
        _token: &FactoryToken,
        input: Box<dyn ByteReader>,
        request: ReadRequest,
        (schema, decoder): Self::Prepared,
    ) -> Result<Box<dyn RecordReader>> {
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

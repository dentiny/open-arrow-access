use super::{CsvFormat, csv_supports_type, reader::CsvReader};
use crate::FileFormat;
use arrow_csv::{ReaderBuilder, reader::Decoder};
use open_arrow_access_core::{
    ByteReader, Error, ErrorKind, ReadOptions, ReadRequest, RecordMetadata, RecordReader, Result,
    SchemaRef, async_trait,
};

/// Incrementally read CSV or TSV using an explicit positional schema.
/// Defaults to comma-separated input without a header; use [`Self::with_format`]
/// to select a tab delimiter, header handling and other Arrow CSV options.
#[derive(Clone, Debug)]
pub struct CsvFileFormat {
    schema: SchemaRef,
    format: CsvFormat,
}

impl CsvFileFormat {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            format: CsvFormat::default(),
        }
    }

    pub fn with_format(mut self, format: CsvFormat) -> Self {
        self.format = format;
        self
    }
}

#[async_trait]
impl FileFormat for CsvFileFormat {
    type Prepared = (SchemaRef, Decoder);

    fn prepare(&self, options: &ReadOptions) -> Result<Self::Prepared> {
        if self.schema.fields().is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "open_csv",
                "CSV requires an input schema with at least one field",
            ));
        }
        let (schema, projection) = options.resolve_projection(&self.schema)?;
        // Arrow CSV reports unsupported field types as parse errors at flush time.
        // Validate selected fields here so configuration failures are not Decode errors.
        for field in schema.fields() {
            if !csv_supports_type(field.data_type()) {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "open_csv",
                    format!(
                        "CSV does not support field {:?} of type {:?}",
                        field.name(),
                        field.data_type()
                    ),
                ));
            }
        }
        let mut builder = ReaderBuilder::new(self.schema.clone())
            .with_format(self.format.clone())
            .with_batch_size(options.batch_size.get());
        if let Some(indices) = projection {
            builder = builder.with_projection(indices);
        }
        Ok((schema, builder.build_decoder()))
    }

    async fn open(
        &self,
        input: Box<dyn ByteReader>,
        request: ReadRequest,
        (schema, decoder): Self::Prepared,
    ) -> Result<Box<dyn RecordReader>> {
        Ok(Box::new(CsvReader::new(
            input,
            decoder,
            RecordMetadata {
                schema,
                source: Some(request.object),
            },
        )))
    }
}

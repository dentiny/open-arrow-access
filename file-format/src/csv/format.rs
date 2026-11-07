use super::{CsvFormat, reader::csv_reader};
use arrow_csv::{ReaderBuilder, reader::Decoder};
use arrow_schema::{DataType, TimeUnit};
use open_arrow_access_core::{
    ByteReader, Error, ErrorKind, ReadRequest, ReaderFactory, RecordMetadata, RecordReader, Result,
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
impl ReaderFactory for CsvFileFormat {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    type Prepared = (SchemaRef, Decoder);

    fn prepare(&self, request: &ReadRequest) -> Result<Self::Prepared> {
        let options = &request.options;
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
            if !supported_type(field.data_type()) {
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

    async fn create_reader(
        &self,
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

fn supported_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
        | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond)
        | DataType::Timestamp(_, _)
        | DataType::Null
        | DataType::Utf8
        | DataType::Utf8View => true,
        DataType::Dictionary(key, value) => {
            value.as_ref() == &DataType::Utf8
                && matches!(
                    key.as_ref(),
                    DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64
                )
        }
        _ => false,
    }
}

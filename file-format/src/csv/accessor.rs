use arrow_csv::ReaderBuilder;
use arrow_schema::{DataType, TimeUnit};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, Error, ErrorKind, ReadRequest,
    RecordMetadata, RecordReader, Result, SchemaRef, async_trait,
};

use super::{CsvFormat, reader::CsvReader};

/// Incrementally read CSV or TSV using an explicit positional schema.
/// Defaults to comma-separated input without a header; use [`Self::with_format`]
/// to select a tab delimiter, header handling and other Arrow CSV options.
#[derive(Clone, Debug)]
pub struct CsvAccessor<A> {
    inner: A,
    schema: SchemaRef,
    format: CsvFormat,
}

impl<A> CsvAccessor<A> {
    pub fn new(inner: A, schema: SchemaRef) -> Self {
        Self {
            inner,
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
impl<A> Accessor for CsvAccessor<A>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    type Request = ReadRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> Capability {
        Capability {
            read: self.inner.capability().read,
            read_with_range: false,
            list: false,
        }
    }

    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        if self.schema.fields().is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "open_csv",
                "CSV requires an input schema with at least one field",
            ));
        }
        let (schema, projection) = request.options.resolve_projection(&self.schema)?;
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
            .with_batch_size(request.options.batch_size.get());
        if let Some(indices) = projection {
            builder = builder.with_projection(indices);
        }
        let input = self
            .inner
            .open(ByteReadRequest::new(request.object.clone()).with_options(request.byte_options))
            .await?;
        Ok(Box::new(CsvReader::new(
            input,
            builder.build_decoder(),
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

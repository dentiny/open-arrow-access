use arrow_csv::ReaderBuilder;
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
            ..Capability::default()
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

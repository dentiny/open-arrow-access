use arrow_json::ReaderBuilder;
use arrow_schema::ArrowError;
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, Error, ErrorKind, ReadRequest,
    RecordMetadata, RecordReader, Result, SchemaRef, async_trait,
};

use super::reader::JsonReader;

/// Read whitespace-separated JSON objects, including JSONL, with a fixed schema.
///
/// Missing nullable fields become null; unknown fields are ignored. Top-level
/// arrays and scalar records are rejected. Schema inference is not performed.
/// Reading requires only sequential bytes, so decompression wrappers can be used.
#[derive(Clone, Debug)]
pub struct JsonAccessor<A> {
    inner: A,
    schema: SchemaRef,
}

impl<A> JsonAccessor<A> {
    pub fn new(inner: A, schema: SchemaRef) -> Self {
        Self { inner, schema }
    }
}

#[async_trait]
impl<A> Accessor for JsonAccessor<A>
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
        let (schema, _) = request.options.resolve_projection(&self.schema)?;
        let decoder = ReaderBuilder::new(schema.clone())
            .with_batch_size(request.options.batch_size.get())
            .build_decoder()
            .map_err(|error| {
                let kind = match &error {
                    ArrowError::NotYetImplemented(_) => ErrorKind::Unsupported,
                    _ => ErrorKind::InvalidInput,
                };
                Error::new(kind, "open_json", error.to_string()).with_source(error)
            })?;
        let input = self
            .inner
            .open(ByteReadRequest::new(request.object.clone()).with_options(request.byte_options))
            .await?;
        Ok(Box::new(JsonReader::new(
            input,
            decoder,
            RecordMetadata {
                schema,
                source: Some(request.object),
            },
            request.options.batch_size.get(),
        )))
    }
}

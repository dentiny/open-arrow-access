use std::sync::Arc;

use futures::stream;

use crate::{
    Accessor, ByteReadRequest, ByteReader, Capability, Error, ErrorKind, ReadOptionsProvider,
    RecordBatch, RecordMetadata, RecordReader, Result, SchemaRef, StreamReader, async_trait,
    read_object,
};

/// Decode one whole object into one Arrow row.
#[async_trait]
pub trait DocumentDecoder: Send + Sync {
    /// Per-document request and read options.
    type Request: ReadOptionsProvider + 'static;

    /// Validate without I/O and return the full schema.
    fn prepare(&self, request: &Self::Request) -> Result<SchemaRef>;

    /// Validate all input and return one row with the projected schema.
    /// Related-object limits belong to the decoder.
    async fn decode(
        &self,
        bytes: &[u8],
        request: Self::Request,
        schema: SchemaRef,
    ) -> Result<RecordBatch>;
}

/// Read one bounded document into one Arrow row.
/// Schema is available at open; input I/O starts on the first `next()`.
pub struct DocumentAccessor<B, D> {
    storage: Arc<B>,
    decoder: Arc<D>,
    max_bytes: usize,
}

impl<B, D> DocumentAccessor<B, D> {
    /// Limit collected input bytes; decoder allocations are separate.
    pub fn new(storage: B, decoder: D, max_bytes: usize) -> Self {
        Self {
            storage: Arc::new(storage),
            decoder: Arc::new(decoder),
            max_bytes,
        }
    }
}

#[async_trait]
impl<B, D> Accessor for DocumentAccessor<B, D>
where
    B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static,
    D: DocumentDecoder + 'static,
    for<'a> ByteReadRequest: From<&'a D::Request>,
{
    type Request = D::Request;
    type Reader = dyn RecordReader;

    fn capability(&self) -> Capability {
        Capability {
            read: self.storage.capability().read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    async fn open(&self, request: D::Request) -> Result<Box<dyn RecordReader>> {
        let full_schema = self.decoder.prepare(&request)?;
        let (schema, _) = request.read_options().resolve_projection(&full_schema)?;
        let byte_request = ByteReadRequest::from(&request);
        let metadata = RecordMetadata {
            schema: schema.clone(),
            source: Some(byte_request.object.clone()),
        };
        let storage = self.storage.clone();
        let decoder = self.decoder.clone();
        let max_bytes = self.max_bytes;
        let stream = stream::once(async move {
            let bytes =
                read_object(storage.as_ref(), byte_request, max_bytes, "read_document").await?;
            let batch = decoder.decode(&bytes, request, schema.clone()).await?;
            if batch.num_rows() != 1 || batch.schema() != schema {
                return Err(Error::new(
                    ErrorKind::Decode,
                    "decode_document",
                    "decoder must return one row with the projected schema",
                ));
            }
            batch.with_schema(schema).map_err(|error| {
                Error::new(ErrorKind::Decode, "decode_document", error.to_string())
                    .with_source(error)
            })
        });
        Ok(Box::new(StreamReader::new(stream, metadata)))
    }
}

use std::sync::Arc;

use futures::stream;

use crate::{
    Accessor, ByteReadRequest, ByteReader, Capability, Error, ErrorKind, ReadOptionsProvider,
    RecordBatch, RecordMetadata, RecordReader, Result, ScanPlan, ScanPlanner, SchemaRef,
    StreamReader, async_trait, read_object,
};

/// Decode one whole object into one Arrow row.
#[async_trait]
pub trait DocumentDecoder: Send + Sync {
    /// Per-document request and read options.
    type Request: ReadOptionsProvider + 'static;

    /// Validate without I/O and return the full schema.
    fn prepare(&self, request: &Self::Request) -> Result<SchemaRef>;

    /// Validate all input and return one row with the projected schema.
    async fn decode(
        &self,
        bytes: &[u8],
        request: Self::Request,
        schema: SchemaRef,
    ) -> Result<RecordBatch>;
}

/// Read one document into one Arrow row.
/// Schema is available at open; input I/O starts on the first `next()`.
pub struct DocumentAccessor<B, D> {
    storage: Arc<B>,
    decoder: Arc<D>,
}

impl<B, D: Default> DocumentAccessor<B, D> {
    /// Construct a document provider with its default decoder settings.
    pub fn with_default_decoder(storage: B) -> Self {
        Self::new(storage, D::default())
    }
}

impl<B, D> Clone for DocumentAccessor<B, D> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            decoder: self.decoder.clone(),
        }
    }
}

impl<B, D> DocumentAccessor<B, D> {
    pub fn new(storage: B, decoder: D) -> Self {
        Self {
            storage: Arc::new(storage),
            decoder: Arc::new(decoder),
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
        let stream = stream::once(async move {
            let bytes = read_object(storage.as_ref(), byte_request, "read_document").await?;
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

#[async_trait]
impl<B, D> ScanPlanner for DocumentAccessor<B, D>
where
    B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static,
    D: DocumentDecoder + 'static,
    for<'a> ByteReadRequest: From<&'a D::Request>,
{
    async fn plan_scan(&self, request: D::Request) -> Result<ScanPlan> {
        Ok(ScanPlan::whole_input(self.open(request).await?))
    }
}

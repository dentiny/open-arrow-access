use std::sync::Arc;

use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, DocumentAccessor, DocumentDecoder,
    ReadRequest, RecordBatch, RecordReader, Result, SchemaRef, async_trait,
};

use super::{metadata, schema};

/// Read one article-version JSON object; content URLs remain metadata.
pub struct PmcAccessor<A> {
    inner: Arc<A>,
}
impl<A> Clone for PmcAccessor<A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl<A> PmcAccessor<A> {
    pub fn new(inner: A) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

struct PmcDecoder;

#[async_trait]
impl DocumentDecoder for PmcDecoder {
    type Request = ReadRequest;
    fn prepare(&self, _: &ReadRequest) -> Result<SchemaRef> {
        Ok(schema())
    }
    async fn decode(
        &self,
        bytes: &[u8],
        request: ReadRequest,
        schema: SchemaRef,
    ) -> Result<RecordBatch> {
        metadata::decode_batch(bytes, request.object.locator(), schema)
    }
}

#[async_trait]
impl<A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static> Accessor
    for PmcAccessor<A>
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
        DocumentAccessor::new(self.inner.clone(), PmcDecoder)
            .open(request)
            .await
    }
}

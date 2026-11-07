use super::{PmcLimits, capability, reader::create_reader, schema};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, ReadRequest, RecordMetadata, RecordReader,
    Result, async_trait,
};
use std::sync::Arc;

/// Read one article-version JSON object. Discovery and content retrieval are separate.
pub struct PmcAccessor<A> {
    inner: Arc<A>,
    limits: PmcLimits,
}
impl<A> Clone for PmcAccessor<A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            limits: self.limits,
        }
    }
}
impl<A> PmcAccessor<A> {
    pub fn new(inner: A) -> Self {
        Self {
            inner: Arc::new(inner),
            limits: PmcLimits::default(),
        }
    }
    pub fn with_limits(mut self, limits: PmcLimits) -> Self {
        self.limits = limits;
        self
    }
}
#[async_trait]
impl<A> Accessor for PmcAccessor<A>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static,
{
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    fn capability(&self) -> Capability {
        capability(self.inner.capability().read)
    }
    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        let (schema, _) = request.options.resolve_projection(&schema())?;
        let metadata = RecordMetadata {
            schema,
            source: Some(request.object.clone()),
        };
        Ok(Box::new(create_reader(
            self.inner.clone(),
            request,
            self.limits,
            metadata,
        )))
    }
}

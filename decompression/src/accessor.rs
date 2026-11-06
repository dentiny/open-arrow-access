use std::num::NonZeroUsize;

use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, ByteReader, Capability, ListRequest, ObjectStream,
    Result, async_trait,
};

use crate::{Codec, reader::DecompressionReader};

/// Byte accessor decorated by a [`crate::DecompressionLayer`].
#[derive(Clone, Debug)]
pub struct DecompressionAccessor<A, C> {
    inner: A,
    codec: C,
    chunk_size: NonZeroUsize,
}

impl<A, C> DecompressionAccessor<A, C> {
    pub(crate) fn new(inner: A, codec: C, chunk_size: NonZeroUsize) -> Self {
        Self {
            inner,
            codec,
            chunk_size,
        }
    }
}

#[async_trait]
impl<A: ByteAccessor, C: Codec> Accessor for DecompressionAccessor<A, C> {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        let capability = self.inner.capability();
        Capability {
            read: capability.read,
            read_with_range: false,
            read_parallel: false,
            list: capability.list,
        }
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        let inner = self.inner.open(request).await?;
        Ok(Box::new(DecompressionReader::new(
            inner,
            &self.codec,
            self.chunk_size,
        )))
    }
}

#[async_trait]
impl<A: ByteAccessor, C: Codec> ByteAccessor for DecompressionAccessor<A, C> {
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        self.inner.list(request).await
    }
}

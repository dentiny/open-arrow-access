use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, ByteReader, Capability, ListRequest, ObjectStream,
    ReaderFactory, ReaderFactoryExt, Result, async_trait,
};

use crate::{Codec, DecompressionFactory};

/// Byte accessor decorated by a [`crate::DecompressionLayer`].
#[derive(Clone, Debug)]
pub struct DecompressionAccessor<A, C> {
    inner: A,
    factory: DecompressionFactory<C>,
}

impl<A, C> DecompressionAccessor<A, C> {
    pub(crate) fn new(inner: A, factory: DecompressionFactory<C>) -> Self {
        Self { inner, factory }
    }
}

#[async_trait]
impl<A: ByteAccessor, C: Codec> Accessor for DecompressionAccessor<A, C> {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        let input = self.inner.capability();
        let mut output = self.factory.capability(input);
        output.list = input.list;
        output
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        ReaderFactoryExt::open_reader(&self.factory, &self.inner, request).await
    }
}

#[async_trait]
impl<A: ByteAccessor, C: Codec> ByteAccessor for DecompressionAccessor<A, C> {
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        self.inner.list(request).await
    }
}

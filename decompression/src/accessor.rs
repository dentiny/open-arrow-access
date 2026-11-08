use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, ByteReader, Capability, FactoryAccessor, ListRequest,
    ObjectStream, Result, async_trait,
};

use crate::{Codec, DecompressionFactory};

/// Byte accessor decorated by a [`crate::DecompressionLayer`].
#[derive(Clone, Debug)]
pub struct DecompressionAccessor<A, C> {
    binding: FactoryAccessor<A, DecompressionFactory<C>>,
}

impl<A, C> DecompressionAccessor<A, C> {
    pub(crate) fn new(inner: A, factory: DecompressionFactory<C>) -> Self {
        Self {
            binding: FactoryAccessor::new(inner, factory),
        }
    }
}

#[async_trait]
impl<A: ByteAccessor, C: Codec> Accessor for DecompressionAccessor<A, C> {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        let mut output = self.binding.capability();
        output.list = self.binding.storage().capability().list;
        output
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        self.binding.open(request).await
    }
}

#[async_trait]
impl<A: ByteAccessor, C: Codec> ByteAccessor for DecompressionAccessor<A, C> {
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        self.binding.storage().list(request).await
    }
}

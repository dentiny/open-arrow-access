use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, ByteReader, Capability, FactoryAccessor, ListRequest,
    ObjectStream, ReaderFactory, Result, async_trait,
};

use crate::DecompressionFactory;

/// Byte accessor decorated by a [`crate::DecompressionLayer`].
pub type DecompressionAccessor<A, C> = DecodedAccessor<A, DecompressionFactory<C>>;

/// Bind byte storage to a decoding factory while forwarding storage listing.
///
/// [`DecompressionAccessor`] and [`crate::AutoDecompressionAccessor`] select the
/// explicit-codec and suffix-routing factories respectively. Reader capabilities
/// come from the factory; listing remains a capability of the underlying storage.
#[derive(Clone, Debug)]
pub struct DecodedAccessor<A, F> {
    binding: FactoryAccessor<A, F>,
}

impl<A, F> DecodedAccessor<A, F> {
    pub(crate) fn new(inner: A, factory: F) -> Self {
        Self {
            binding: FactoryAccessor::new(inner, factory),
        }
    }
}

#[async_trait]
impl<A, F> Accessor for DecodedAccessor<A, F>
where
    A: ByteAccessor,
    F: ReaderFactory<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
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
impl<A, F> ByteAccessor for DecodedAccessor<A, F>
where
    A: ByteAccessor,
    F: ReaderFactory<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        self.binding.storage().list(request).await
    }
}

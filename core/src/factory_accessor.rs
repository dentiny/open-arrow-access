use crate::{
    Accessor, ByteAccessor, ByteReadRequest, ByteReader, Capability, ReaderFactory,
    ReaderFactoryExt, Result, async_trait,
};

/// Bind byte storage to a reader factory through the shared opening protocol.
///
/// Factory preparation precedes storage I/O. Request and output types belong to
/// the factory; listing remains a responsibility of the enclosing accessor.
#[derive(Clone, Debug)]
pub struct FactoryAccessor<B, F> {
    storage: B,
    factory: F,
}

impl<B, F> FactoryAccessor<B, F> {
    pub fn new(storage: B, factory: F) -> Self {
        Self { storage, factory }
    }
    pub fn storage(&self) -> &B {
        &self.storage
    }
    pub fn factory(&self) -> &F {
        &self.factory
    }
    pub fn factory_mut(&mut self) -> &mut F {
        &mut self.factory
    }
    pub fn map_factory(self, map: impl FnOnce(F) -> F) -> Self {
        Self {
            storage: self.storage,
            factory: map(self.factory),
        }
    }
}

#[async_trait]
impl<B, F> Accessor for FactoryAccessor<B, F>
where
    B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
    F: ReaderFactory,
    for<'a> ByteReadRequest: From<&'a F::Request>,
{
    type Request = F::Request;
    type Reader = F::Reader;
    fn capability(&self) -> Capability {
        self.factory.capability(self.storage.capability())
    }
    async fn open(&self, request: Self::Request) -> Result<Box<Self::Reader>> {
        self.factory.open_reader(&self.storage, request).await
    }
}

#[async_trait]
impl<B, F> ByteAccessor for FactoryAccessor<B, F>
where
    B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
    F: ReaderFactory<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
}

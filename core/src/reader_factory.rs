use crate::{Accessor, ByteReadRequest, ByteReader, Capability, Reader, Result, async_trait};

/// Supplied by [`ReaderFactoryExt::open_reader`] or [`ReaderFactoryExt::open_input`] to creation hooks.
/// Public so other crates can implement hooks; only this module can construct it.
pub struct FactoryToken {
    _private: (),
}

/// Create an independently owned reader from one opened byte input.
/// [`ReaderFactoryExt::open_reader`] prepares configuration before opening storage.
#[async_trait]
pub trait ReaderFactory: Send + Sync {
    /// Typed selection and configuration for one open operation.
    type Request: Send;
    /// Per-open decoder or configuration state, prepared without I/O.
    type Prepared: Send;
    /// Output interface, including its item and metadata types.
    type Reader: Reader + ?Sized + 'static;

    /// Derive output capabilities without I/O; listing belongs to the accessor.
    /// Required for every factory: record formats must explicitly declare ordering.
    fn capability(&self, input: Capability) -> <Self::Reader as Reader>::Capability;

    /// Validate configuration before storage opens; return configuration errors here.
    fn prepare(&self, request: &Self::Request) -> Result<Self::Prepared>;

    /// Select storage opening hints without changing the caller's I/O options.
    fn byte_request(&self, request: &Self::Request) -> ByteReadRequest
    where
        for<'a> ByteReadRequest: From<&'a Self::Request>,
    {
        ByteReadRequest::from(request)
    }

    /// Inspect output metadata through storage. The default opens and drops a reader;
    /// override to avoid decoder construction and opening side effects when possible.
    async fn metadata(
        &self,
        storage: &dyn Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
        request: Self::Request,
    ) -> Result<<Self::Reader as Reader>::Metadata>
    where
        for<'a> ByteReadRequest: From<&'a Self::Request>,
        <Self::Reader as Reader>::Metadata: Clone,
    {
        Ok(self.open_reader(storage, request).await?.metadata().clone())
    }

    /// Return `None` only when bounded inference proves the input empty with no schema.
    /// Explicit schemas and unknown row presence must retain `Some(metadata)`.
    async fn inspect_metadata(
        &self,
        storage: &dyn Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
        request: Self::Request,
    ) -> Result<Option<<Self::Reader as Reader>::Metadata>>
    where
        for<'a> ByteReadRequest: From<&'a Self::Request>,
        <Self::Reader as Reader>::Metadata: Clone,
    {
        Ok(Some(self.metadata(storage, request).await?))
    }

    /// Inspect opened bytes and return an owned reader with metadata available.
    /// Preserve source errors and lifecycle; inspection may perform I/O.
    /// `token` is supplied by the shared [`ReaderFactoryExt`] opening methods.
    async fn create_reader(
        &self,
        token: &FactoryToken,
        input: Box<dyn ByteReader>,
        request: Self::Request,
        prepared: Self::Prepared,
    ) -> Result<Box<Self::Reader>>;
}

/// Shared opening flow provided by one blanket implementation for all factories.
#[async_trait]
pub trait ReaderFactoryExt: ReaderFactory {
    /// Prepare a format and decode an already-open byte reader. Preparation
    /// finishes before the factory can poll the supplied input.
    async fn open_input(
        &self,
        input: Box<dyn ByteReader>,
        request: Self::Request,
    ) -> Result<Box<Self::Reader>>;

    /// Prepare, open storage, and create a reader; implementations cannot override it.
    async fn open_reader(
        &self,
        storage: &dyn Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
        request: Self::Request,
    ) -> Result<Box<Self::Reader>>
    where
        for<'a> ByteReadRequest: From<&'a Self::Request>;
}

#[async_trait]
impl<F: ReaderFactory + ?Sized> ReaderFactoryExt for F {
    async fn open_input(
        &self,
        input: Box<dyn ByteReader>,
        request: Self::Request,
    ) -> Result<Box<Self::Reader>> {
        let object = input.metadata().source.clone();
        let context = |error: crate::Error| error.with_object(object.as_ref());
        let prepared = self.prepare(&request).map_err(context)?;
        self.create_reader(&FactoryToken { _private: () }, input, request, prepared)
            .await
            .map_err(context)
    }

    async fn open_reader(
        &self,
        storage: &dyn Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
        request: Self::Request,
    ) -> Result<Box<Self::Reader>>
    where
        for<'a> ByteReadRequest: From<&'a Self::Request>,
    {
        let byte_request = self.byte_request(&request);
        let object = byte_request.object.clone();
        let context = |error: crate::Error| error.with_object(Some(&object));
        let prepared = self.prepare(&request).map_err(context)?;
        let input = storage.open(byte_request).await.map_err(context)?;
        let token = FactoryToken { _private: () };
        self.create_reader(&token, input, request, prepared)
            .await
            .map_err(context)
    }
}

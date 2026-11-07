use crate::{Accessor, ByteReadRequest, ByteReader, Capability, Reader, Result, async_trait};

/// Supplied by [`ReaderFactory::open_reader`] to creation hooks.
/// Public so other crates can implement hooks; only this module can construct it.
pub struct FactoryToken {
    _private: (),
}

/// Create an independently owned reader from one opened byte input.
/// [`Self::open_reader`] prepares configuration before opening storage.
#[async_trait]
pub trait ReaderFactory: Send + Sync {
    /// Typed selection and configuration for one open operation.
    type Request: Send;
    /// Per-open decoder or configuration state, prepared without I/O.
    type Prepared: Send;
    /// Output interface, including its item and metadata types.
    type Reader: Reader + ?Sized + 'static;

    /// Derive output capabilities without I/O; listing belongs to the accessor.
    fn capability(&self, input: Capability) -> Capability {
        Capability {
            read: input.read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    /// Validate configuration before storage opens; return configuration errors here.
    fn prepare(&self, request: &Self::Request) -> Result<Self::Prepared>;

    /// Inspect opened bytes and return an owned reader with metadata available.
    /// Preserve source errors and lifecycle; inspection may perform I/O.
    /// `token` is supplied by [`Self::open_reader`].
    async fn create_reader(
        &self,
        token: &FactoryToken,
        input: Box<dyn ByteReader>,
        request: Self::Request,
        prepared: Self::Prepared,
    ) -> Result<Box<Self::Reader>>;

    /// Prepare, open storage, and create a reader, forwarding errors unchanged.
    async fn open_reader(
        &self,
        storage: &dyn Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
        request: Self::Request,
    ) -> Result<Box<Self::Reader>>
    where
        for<'a> ByteReadRequest: From<&'a Self::Request>,
    {
        let prepared = self.prepare(&request)?;
        let byte_request = ByteReadRequest::from(&request);
        let input = storage.open(byte_request).await?;
        let token = FactoryToken { _private: () };
        self.create_reader(&token, input, request, prepared).await
    }
}

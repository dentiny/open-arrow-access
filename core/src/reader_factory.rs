use crate::{Accessor, ByteReadRequest, ByteReader, Capability, Reader, Result, async_trait};

/// Create an independently owned reader from one opened byte input.
/// Storage opening belongs to [`open_reader`]; decoding and inspection belong here.
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

    /// Inspect opened bytes and create a reader with metadata available on return.
    /// Preserve source errors and the reader lifecycle; the output cannot borrow
    /// the factory or prepared state. Input inspection may perform I/O.
    async fn create_reader(
        &self,
        input: Box<dyn ByteReader>,
        request: Self::Request,
        prepared: Self::Prepared,
    ) -> Result<Box<Self::Reader>>;
}

/// Prepare, open storage, and create a reader, forwarding errors unchanged.
/// Request conversion preserves the selected object and per-open byte options.
/// Dropping this future releases owned preparation/input state without background work.
pub async fn open_reader<B, F>(
    storage: &B,
    factory: &F,
    request: F::Request,
) -> Result<Box<F::Reader>>
where
    B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + ?Sized,
    F: ReaderFactory + ?Sized,
    for<'a> ByteReadRequest: From<&'a F::Request>,
{
    let prepared = factory.prepare(&request)?;
    let byte_request = ByteReadRequest::from(&request);
    let input = storage.open(byte_request).await?;
    factory.create_reader(input, request, prepared).await
}

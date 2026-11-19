use crate::{Accessor, ByteReadRequest, ByteReader, Result, read_all};

/// Open an object sequentially and collect its bytes using [`read_all`].
/// Backends may avoid eager metadata I/O through `Accessor::open_sequential`.
/// Preserve request options and source errors. Unsupported reads fail before I/O.
pub async fn read_object<A>(
    accessor: &A,
    request: ByteReadRequest,
    operation: &'static str,
) -> Result<Vec<u8>>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + ?Sized,
{
    crate::validation::validate_read_capability(accessor.capability().read, operation)?;
    let input = accessor.open_sequential(request).await?;
    read_all(input, operation).await
}

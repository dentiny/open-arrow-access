use crate::{Accessor, ByteReadRequest, ByteReader, Result, read_all, read_all_limited};

/// Open an object and collect its bytes using [`read_all`].
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
    let input = accessor.open(request).await?;
    read_all(input, operation).await
}

/// Open and collect at most `max_bytes`, preserving request options and source errors.
/// Like [`read_all_limited`], rejects actual accumulated bytes beyond the limit and
/// drops the input immediately. Existing [`read_object`] remains unlimited.
pub async fn read_object_limited<A>(
    accessor: &A,
    request: ByteReadRequest,
    operation: &'static str,
    max_bytes: usize,
) -> Result<Vec<u8>>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + ?Sized,
{
    crate::validation::validate_read_capability(accessor.capability().read, operation)?;
    let input = accessor.open(request).await?;
    read_all_limited(input, operation, max_bytes).await
}

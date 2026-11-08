use crate::{Accessor, ByteReadRequest, ByteReader, Error, ErrorKind, Result, read_limited};

/// Open an object and collect at most `limit` bytes using [`read_limited`].
/// Preserve request options and source errors. Unsupported reads fail before I/O.
pub async fn read_object<A>(
    accessor: &A,
    request: ByteReadRequest,
    limit: usize,
    operation: &'static str,
) -> Result<Vec<u8>>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + ?Sized,
{
    if !accessor.capability().read {
        return Err(Error::new(
            ErrorKind::Unsupported,
            operation,
            "accessor does not support reading",
        ));
    }
    let input = accessor.open(request).await?;
    read_limited(input, limit, operation).await
}

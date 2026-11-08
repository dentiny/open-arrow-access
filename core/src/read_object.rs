use crate::{Accessor, ByteReadRequest, ByteReader, Error, ErrorKind, Result, read_all};

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
    if !accessor.capability().read {
        return Err(Error::new(
            ErrorKind::Unsupported,
            operation,
            "accessor does not support reading",
        ));
    }
    let input = accessor.open(request).await?;
    read_all(input, operation).await
}

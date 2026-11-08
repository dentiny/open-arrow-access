use crate::{ByteReader, Error, ErrorKind, Result};

/// Collect an owned byte reader, preserving source errors and rejecting empty chunks.
pub async fn read_all<R>(mut reader: Box<R>, operation: &'static str) -> Result<Vec<u8>>
where
    R: ByteReader + ?Sized,
{
    let mut output = Vec::new();
    while let Some(bytes) = reader.next().await? {
        if bytes.is_empty() {
            return Err(Error::new(
                ErrorKind::Io,
                operation,
                "byte reader returned an empty chunk",
            ));
        }
        output.extend_from_slice(&bytes);
    }
    Ok(output)
}

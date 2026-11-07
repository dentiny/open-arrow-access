use crate::{ByteReader, Error, ErrorKind, Result};

/// Collect an owned byte reader without exceeding `limit` output bytes.
///
/// Pass a newly opened reader: its advertised length is checked before reading.
/// Actual bytes are checked before copying, even when the length is unknown.
/// A zero limit accepts only empty input. The limit does not bound upstream
/// buffers or the vector's allocation capacity. Completion or cancellation drops
/// the owned reader; no background tasks are started.
///
/// # Errors
///
/// Exceeding the limit returns [`ErrorKind::Decode`]; an empty chunk returns
/// [`ErrorKind::Io`]. Both use `operation`. Upstream errors pass through unchanged.
///
/// ```no_run
/// use open_arrow_access_core::{ByteReader, Result, read_limited};
///
/// # async fn read_manifest(input: Box<dyn ByteReader>) -> Result<()> {
/// let bytes = read_limited(input, 4 * 1024 * 1024, "read_manifest").await?;
/// // Parse the bounded bytes here.
/// # Ok(())
/// # }
/// ```
pub async fn read_limited<R>(
    mut reader: Box<R>,
    limit: usize,
    operation: &'static str,
) -> Result<Vec<u8>>
where
    R: ByteReader + ?Sized,
{
    let limit_error = || {
        Error::new(
            ErrorKind::Decode,
            operation,
            format!("input exceeds configured {limit}-byte limit"),
        )
    };
    if reader
        .metadata()
        .content_length
        .is_some_and(|length| length > limit as u64)
    {
        return Err(limit_error());
    }

    let mut output = Vec::new();
    while let Some(bytes) = reader.next().await? {
        if bytes.is_empty() {
            return Err(Error::new(
                ErrorKind::Io,
                operation,
                "byte reader returned an empty chunk",
            ));
        }
        if bytes.len() > limit - output.len() {
            return Err(limit_error());
        }
        output.extend_from_slice(&bytes);
    }
    Ok(output)
}

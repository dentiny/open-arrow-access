use std::io;

use futures::{TryStreamExt, io::AsyncBufRead};

use crate::{ByteReader, Error, ErrorKind};

/// Adapt an owned byte reader to the futures I/O traits used by streaming parsers.
///
/// Construction performs no I/O. The adapter retains each input chunk until it
/// is consumed, without collecting the input or starting a background task.
/// Dropping it drops the owned reader. Only sequential reads are exposed.
///
/// EOF and errors are terminal, including an empty input chunk, which violates
/// the [`ByteReader`] contract. Upstream errors are retained inside
/// [`io::Error::other`] so their categories and source chains can be recovered
/// with [`ErrorKind::from_source`]. `operation` supplies context for empty-chunk
/// errors; upstream errors keep their original context.
///
/// ```no_run
/// use futures::io::AsyncBufReadExt;
/// use open_arrow_access_core::{ByteReader, into_async_read};
///
/// # async fn read_line(input: Box<dyn ByteReader>) -> std::io::Result<()> {
/// let mut input = into_async_read(input, "read_manifest");
/// let mut line = String::new();
/// input.read_line(&mut line).await?;
/// # Ok(())
/// # }
/// ```
pub fn into_async_read<R>(
    reader: Box<R>,
    operation: &'static str,
) -> impl AsyncBufRead + Unpin + Send
where
    R: ByteReader + ?Sized + 'static,
{
    let stream = futures::stream::try_unfold(reader, move |mut reader| async move {
        let Some(bytes) = reader.next().await.map_err(io::Error::other)? else {
            return Ok(None);
        };
        if bytes.is_empty() {
            return Err(io::Error::other(Error::new(
                ErrorKind::Io,
                operation,
                "byte reader returned an empty chunk",
            )));
        }
        Ok(Some((bytes, reader)))
    });
    Box::pin(stream).into_async_read()
}

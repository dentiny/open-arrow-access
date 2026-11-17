use std::{
    io,
    pin::Pin,
    task::{Context, Poll, ready},
};

use futures::{TryStreamExt, io::AsyncBufRead};

use crate::ByteReader;

/// Adapt an owned byte reader to the futures I/O traits used by streaming parsers.
///
/// Lazily consumes one chunk at a time without read-ahead or background tasks.
/// Dropping the adapter drops its reader; only sequential reads are exposed.
/// EOF, source errors and empty chunks terminate input.
///
/// Source errors retain their context inside [`io::Error::other`]; recover their
/// categories and chains with [`crate::ErrorKind::from_source`]. `operation` supplies
/// context for empty-chunk errors.
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
        let Some(bytes) = crate::next_nonempty(reader.as_mut(), || crate::empty_chunk(operation))
            .await
            .map_err(io::Error::other)?
        else {
            return Ok(None);
        };
        Ok(Some((bytes, reader)))
    });
    Box::pin(stream).into_async_read()
}

/// Copy available buffered bytes and advance through the adapter's own consume hook.
pub(crate) fn poll_read_from_buf<R: AsyncBufRead + ?Sized>(
    mut reader: Pin<&mut R>,
    cx: &mut Context<'_>,
    output: &mut [u8],
) -> Poll<io::Result<usize>> {
    if output.is_empty() {
        return Poll::Ready(Ok(0));
    }
    let bytes = ready!(reader.as_mut().poll_fill_buf(cx))?;
    let count = bytes.len().min(output.len());
    output[..count].copy_from_slice(&bytes[..count]);
    reader.consume(count);
    Poll::Ready(Ok(count))
}

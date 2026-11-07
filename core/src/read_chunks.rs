use std::{io, num::NonZeroUsize};

use futures::{Stream, io::AsyncRead, io::AsyncReadExt, stream};

use crate::Bytes;

/// Stream nonempty byte chunks of at most `chunk_size` from an owned async reader.
///
/// Construction performs no reads. Short reads yield immediately; EOF and errors
/// terminate the stream and drop the input. I/O errors pass through unchanged.
/// The input need not be `Unpin`. No read-ahead or background tasks are started.
/// Drop the stream after cancelling an in-flight read.
///
/// ```no_run
/// use std::num::NonZeroUsize;
/// use futures::{TryStreamExt, io::AsyncRead};
/// use open_arrow_access_core::read_chunks;
///
/// # async fn consume(input: impl AsyncRead + Send) -> std::io::Result<()> {
/// let chunks = read_chunks(input, NonZeroUsize::new(64 * 1024).unwrap());
/// futures::pin_mut!(chunks);
/// while let Some(bytes) = chunks.try_next().await? {
///     // Consume the chunk here.
/// }
/// # Ok(())
/// # }
/// ```
pub fn read_chunks<R>(
    input: R,
    chunk_size: NonZeroUsize,
) -> impl Stream<Item = io::Result<Bytes>> + Send
where
    R: AsyncRead + Send,
{
    stream::try_unfold(Box::pin(input), move |mut input| async move {
        let mut output = vec![0; chunk_size.get()];
        let len = input.read(&mut output).await?;
        if len == 0 {
            return Ok(None);
        }
        output.truncate(len);
        Ok(Some((Bytes::from(output), input)))
    })
}

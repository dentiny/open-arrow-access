use std::{
    io::{self, SeekFrom},
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
};

use crate::{ByteReader, Bytes, Error, ErrorKind, Result, validate_byte_range};
use futures::{
    TryStreamExt,
    io::{AsyncBufRead, AsyncRead, AsyncSeek},
    lock::Mutex,
    stream,
};

type BufferedInput = Pin<Box<dyn AsyncBufRead + Send>>;
type SharedInput = Arc<Mutex<Box<dyn ByteReader>>>;

/// Adapt exact byte ranges to the parser's seek/buffer contract. Each fetch is
/// bounded; seeking discards read-ahead and never replays sequential input.
pub struct SeekableInput {
    reader: SharedInput,
    len: u64,
    chunk_size: NonZeroUsize,
    operation: &'static str,
    position: u64,
    buffered: Option<BufferedInput>,
}

/// Adapt a ranged, known-length byte reader to futures buffered I/O and seeking.
/// Construction performs no reads. Clones have independent logical cursors and
/// serialize access to the shared source. Drop after cancelling a pending read.
pub fn into_async_seek(
    reader: Box<dyn ByteReader>,
    chunk_size: NonZeroUsize,
    operation: &'static str,
) -> Result<SeekableInput> {
    if !reader.capability().read_with_range {
        return Err(Error::new(
            ErrorKind::Unsupported,
            operation,
            "range reads are required",
        ));
    }
    let len = reader.metadata().content_length.ok_or_else(|| {
        Error::new(
            ErrorKind::Unsupported,
            operation,
            "known input length is required",
        )
    })?;
    Ok(SeekableInput {
        reader: Arc::new(Mutex::new(reader)),
        len,
        chunk_size,
        operation,
        position: 0,
        buffered: None,
    })
}

impl Clone for SeekableInput {
    fn clone(&self) -> Self {
        Self {
            reader: self.reader.clone(),
            len: self.len,
            chunk_size: self.chunk_size,
            operation: self.operation,
            position: self.position,
            buffered: None,
        }
    }
}

impl SeekableInput {
    pub fn content_length(&self) -> u64 {
        self.len
    }

    /// Read an exact range without changing this adapter's cursor or read-ahead.
    pub fn read_range(
        &self,
        range: std::ops::Range<u64>,
    ) -> impl std::future::Future<Output = Result<Bytes>> + Send + 'static + use<> {
        let reader = self.reader.clone();
        let len = self.len;
        let operation = self.operation;
        async move {
            validate_byte_range(&range, len, operation)?;
            exact_range(&reader, range, operation).await
        }
    }
}

async fn exact_range(
    reader: &SharedInput,
    range: std::ops::Range<u64>,
    operation: &'static str,
) -> Result<Bytes> {
    let expected = range.end - range.start;
    let bytes = reader.lock().await.read_range(range).await?;
    if bytes.len() as u64 != expected {
        return Err(Error::new(
            ErrorKind::Io,
            operation,
            "range reader returned an incorrect length",
        ));
    }
    Ok(bytes)
}

impl AsyncBufRead for SeekableInput {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let this = self.get_mut();
        let input = this.buffered.get_or_insert_with(|| {
            let len = this.len;
            let chunk_size = this.chunk_size.get() as u64;
            let operation = this.operation;
            let stream = stream::try_unfold(
                (this.reader.clone(), this.position),
                move |(reader, position)| async move {
                    if position >= len {
                        return Ok(None);
                    }
                    let end = position.saturating_add(chunk_size).min(len);
                    let bytes = exact_range(&reader, position..end, operation)
                        .await
                        .map_err(io::Error::other)?;
                    Ok(Some((bytes, (reader, end))))
                },
            );
            Box::pin(Box::pin(stream).into_async_read())
        });
        input.as_mut().poll_fill_buf(cx)
    }

    fn consume(self: Pin<&mut Self>, amount: usize) {
        let this = self.get_mut();
        if let Some(input) = &mut this.buffered {
            input.as_mut().consume(amount);
            this.position += amount as u64;
        }
    }
}

impl AsyncRead for SeekableInput {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if output.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let bytes = ready!(self.as_mut().poll_fill_buf(cx))?;
        let count = bytes.len().min(output.len());
        output[..count].copy_from_slice(&bytes[..count]);
        self.consume(count);
        Poll::Ready(Ok(count))
    }
}

impl AsyncSeek for SeekableInput {
    fn poll_seek(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        from: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        let this = self.get_mut();
        let position = match from {
            SeekFrom::Start(position) => Some(position),
            SeekFrom::Current(offset) => this.position.checked_add_signed(offset),
            SeekFrom::End(offset) => this.len.checked_add_signed(offset),
        }
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid seek"))?;
        if this.position != position {
            this.position = position;
            this.buffered = None;
        }
        Poll::Ready(Ok(position))
    }
}

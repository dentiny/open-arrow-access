use std::{
    io::{self, SeekFrom},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
};

use futures::{
    TryStreamExt,
    io::{AsyncBufRead, AsyncRead, AsyncSeek},
    lock::Mutex,
    stream,
};
use open_arrow_access_core::{ByteReader, Error, ErrorKind};

type BufferedInput = Pin<Box<dyn AsyncBufRead + Send>>;
pub(super) type SharedInput = Arc<Mutex<Box<dyn ByteReader>>>;

/// Adapt exact byte ranges to the parser's seek/buffer contract. Each fetch is
/// bounded; seeking discards read-ahead and never replays sequential input.
pub(super) struct ZipInput {
    pub(super) reader: SharedInput,
    len: u64,
    position: u64,
    buffered: Option<BufferedInput>,
}

impl ZipInput {
    pub fn new(reader: Box<dyn ByteReader>, len: u64) -> Self {
        Self {
            reader: Arc::new(Mutex::new(reader)),
            len,
            position: 0,
            buffered: None,
        }
    }
}

impl AsyncBufRead for ZipInput {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let this = self.get_mut();
        let input = this.buffered.get_or_insert_with(|| {
            let len = this.len;
            let stream = stream::try_unfold(
                (this.reader.clone(), this.position),
                move |(reader, position)| async move {
                    if position >= len {
                        return Ok(None);
                    }
                    let end = position.saturating_add(64 * 1024).min(len);
                    let bytes = reader
                        .lock()
                        .await
                        .read_range(position..end)
                        .await
                        .map_err(io::Error::other)?;
                    if bytes.len() as u64 != end - position {
                        return Err(io::Error::other(Error::new(
                            ErrorKind::Io,
                            "read_zip",
                            "range reader returned an incorrect length",
                        )));
                    }
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

impl AsyncRead for ZipInput {
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

impl AsyncSeek for ZipInput {
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
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid ZIP seek"))?;
        if this.position != position {
            this.position = position;
            this.buffered = None;
        }
        Poll::Ready(Ok(position))
    }
}

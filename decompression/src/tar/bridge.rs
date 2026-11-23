//! Bridge ranged indexing to synchronous I/O, cancelling reads when dropped.
use futures::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};
use std::{future::Future, io};
use tokio_util::sync::CancellationToken;

pub(super) struct Reader<R> {
    input: R,
    token: CancellationToken,
    handle: tokio::runtime::Handle,
}

pub(super) fn reader<R>(
    input: R,
    token: CancellationToken,
    handle: tokio::runtime::Handle,
) -> Reader<R> {
    Reader {
        input,
        token,
        handle,
    }
}

fn run<T>(
    handle: &tokio::runtime::Handle,
    token: &CancellationToken,
    operation: impl Future<Output = io::Result<T>>,
) -> io::Result<T> {
    handle
        .block_on(token.run_until_cancelled(operation))
        .unwrap_or_else(|| {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "TAR reader dropped",
            ))
        })
}

impl<R: AsyncRead + Unpin> io::Read for Reader<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        run(&self.handle, &self.token, self.input.read(output))
    }
}

impl<R: AsyncSeek + Unpin> io::Seek for Reader<R> {
    fn seek(&mut self, from: io::SeekFrom) -> io::Result<u64> {
        run(&self.handle, &self.token, self.input.seek(from))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{TryStreamExt, channel::oneshot, stream};
    use std::{io::Read, task::Poll};

    #[tokio::test]
    async fn dropping_guard_cancels_pending_read() {
        let (started, waiting) = oneshot::channel();
        let mut started = Some(started);
        let input = stream::poll_fn(move |_| {
            if let Some(started) = started.take() {
                let _ = started.send(());
            }
            Poll::Pending::<Option<io::Result<Vec<u8>>>>
        })
        .into_async_read();
        let token = CancellationToken::new();
        let guard = token.clone().drop_guard();
        let mut input = reader(input, token, tokio::runtime::Handle::current());
        let worker = tokio::task::spawn_blocking(move || input.read(&mut [0; 512]));
        waiting.await.unwrap();
        drop(guard);
        assert_eq!(
            worker.await.unwrap().unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }
}

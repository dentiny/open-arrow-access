//! Bridge ranged indexing to synchronous I/O, cancelling reads when dropped.
use futures::io::{AsyncRead, AsyncSeek};
use std::{
    future::Future,
    io::{self, SeekFrom},
    pin::Pin,
    task::{Context, Poll},
};
use tokio_util::{compat::FuturesAsyncReadCompatExt, io::SyncIoBridge, sync::CancellationToken};

pub(super) struct Cancellable<R> {
    inner: R,
    cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl<R> Cancellable<R> {
    fn check(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "TAR reader dropped",
            ));
        }
        Ok(())
    }
}

type PackageInput<R> = SyncIoBridge<tokio_util::compat::Compat<Cancellable<R>>>;

pub(super) fn reader<R: AsyncRead + Unpin>(
    input: R,
    token: CancellationToken,
    handle: tokio::runtime::Handle,
) -> PackageInput<R> {
    let input = Cancellable {
        inner: input,
        cancelled: Box::pin(token.cancelled_owned()),
    };
    SyncIoBridge::new_with_handle(input.compat(), handle)
}

impl<R: AsyncRead + Unpin> AsyncRead for Cancellable<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.check(cx)?;
        Pin::new(&mut self.inner).poll_read(cx, output)
    }
}

impl<R: AsyncSeek + Unpin> AsyncSeek for Cancellable<R> {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        from: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        self.check(cx)?;
        Pin::new(&mut self.inner).poll_seek(cx, from)
    }
}

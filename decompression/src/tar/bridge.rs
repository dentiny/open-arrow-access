//! Bound package metadata reads; bridge only ranged indexing to synchronous I/O.
use futures::io::{AsyncRead, AsyncSeek};
use std::{
    future::Future,
    io::{self, SeekFrom},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
};
use tokio_util::{compat::FuturesAsyncReadCompatExt, io::SyncIoBridge, sync::CancellationToken};

use super::error;

// Bound bytes consumed while the package resolves one entry's metadata. Body
// reads are unbounded by this counter and retain the per-entry payload limit.
pub(super) const METADATA_BUDGET: u64 = 64 * 1024 + 2 * 512;

pub(super) struct Budget<R> {
    inner: R,
    remaining: Arc<AtomicU64>,
    cancelled: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl<R> Budget<R> {
    fn check(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self
            .cancelled
            .as_mut()
            .is_some_and(|cancelled| cancelled.as_mut().poll(cx).is_ready())
        {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "TAR reader dropped",
            ));
        }
        Ok(())
    }
}

pub(super) fn budget<R>(inner: R) -> (Budget<R>, Arc<AtomicU64>) {
    let remaining = Arc::new(AtomicU64::new(METADATA_BUDGET));
    (
        Budget {
            inner,
            cancelled: None,
            remaining: remaining.clone(),
        },
        remaining,
    )
}

type PackageInput<R> = SyncIoBridge<tokio_util::compat::Compat<Budget<R>>>;

pub(super) fn reader<R: AsyncRead + Unpin>(
    input: R,
    token: CancellationToken,
    handle: tokio::runtime::Handle,
) -> (PackageInput<R>, Arc<AtomicU64>) {
    let (mut input, remaining) = budget(input);
    input.cancelled = Some(Box::pin(token.cancelled_owned()));
    (
        SyncIoBridge::new_with_handle(input.compat(), handle),
        remaining,
    )
}

impl<R: AsyncRead + Unpin> AsyncRead for Budget<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.check(cx)?;
        if output.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let remaining = self.remaining.load(Ordering::Relaxed);
        if remaining == 0 {
            return Poll::Ready(Err(io::Error::other(error::unsupported(
                "TAR entry metadata exceeds the parsing budget",
            ))));
        }
        let limit = output
            .len()
            .min(usize::try_from(remaining).unwrap_or(usize::MAX));
        let read =
            std::task::ready!(Pin::new(&mut self.inner).poll_read(cx, &mut output[..limit]))?;
        if remaining != u64::MAX {
            self.remaining.fetch_sub(read as u64, Ordering::Relaxed);
        }
        Poll::Ready(Ok(read))
    }
}

impl<R: AsyncSeek + Unpin> AsyncSeek for Budget<R> {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        from: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        self.check(cx)?;
        Pin::new(&mut self.inner).poll_seek(cx, from)
    }
}

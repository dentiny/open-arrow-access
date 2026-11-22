//! Adapt asynchronous storage to the package's synchronous Read/Seek API.
use futures::io::{AsyncRead, AsyncSeek};
use std::{
    future::Future,
    io::{self, Read, Seek, SeekFrom},
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

pub(super) struct Lease(pub CancellationToken);
impl Drop for Lease {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

pub(super) struct Cancellable<R> {
    inner: R,
    cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl<R> Cancellable<R> {
    pub(super) fn new(inner: R, token: CancellationToken) -> Self {
        Self {
            inner,
            cancelled: Box::pin(token.cancelled_owned()),
        }
    }

    fn check(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "TAR reader dropped",
            ))
        } else {
            Ok(())
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for Cancellable<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if let Err(error) = self.check(cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.inner).poll_read(cx, output)
    }
}

impl<R: AsyncSeek + Unpin> AsyncSeek for Cancellable<R> {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        from: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        if let Err(error) = self.check(cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.inner).poll_seek(cx, from)
    }
}

pub(super) struct Budget<R> {
    inner: R,
    remaining: Arc<AtomicU64>,
}

type PackageInput<R> = Budget<SyncIoBridge<tokio_util::compat::Compat<Cancellable<R>>>>;

pub(super) fn reader<R: AsyncRead + Unpin>(
    input: R,
    token: CancellationToken,
    handle: tokio::runtime::Handle,
) -> (PackageInput<R>, Arc<AtomicU64>) {
    let remaining = Arc::new(AtomicU64::new(METADATA_BUDGET));
    let bridge = SyncIoBridge::new_with_handle(Cancellable::new(input, token).compat(), handle);
    (
        Budget {
            inner: bridge,
            remaining: remaining.clone(),
        },
        remaining,
    )
}

impl<R: Read> Read for Budget<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        let remaining = self.remaining.load(Ordering::Relaxed);
        if remaining == 0 {
            return Err(io::Error::other(error::unsupported(
                "TAR entry metadata exceeds the parsing budget",
            )));
        }
        let limit = output
            .len()
            .min(usize::try_from(remaining).unwrap_or(usize::MAX));
        let read = self.inner.read(&mut output[..limit])?;
        if remaining != u64::MAX {
            self.remaining.fetch_sub(read as u64, Ordering::Relaxed);
        }
        Ok(read)
    }
}

impl<R: Seek> Seek for Budget<R> {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        self.inner.seek(from)
    }
}

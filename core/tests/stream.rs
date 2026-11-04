use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use futures::{StreamExt, TryStreamExt};
use open_arrow_access_core::{
    BoxFuture, Capability, Error, ErrorKind, Reader, Result, into_stream,
};

struct FailingReader(Arc<AtomicUsize>);

impl Reader for FailingReader {
    type Item = u8;
    type Metadata = ();

    /// Advertise consumption even though the fixture injects a runtime I/O failure.
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            ..Capability::default()
        }
    }

    /// Expose unit metadata for a reader used only to test stream failure handling.
    fn metadata(&self) -> &() {
        &()
    }

    /// Record a poll and inject an error to verify that the stream never polls again.
    fn next(&mut self) -> BoxFuture<'_, Result<Option<u8>>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(Error::new(ErrorKind::Io, "test_read", "injected failure"))
        })
    }
}

#[tokio::test]
async fn stream_stops_after_error_without_polling_reader_again() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut stream = into_stream(Box::new(FailingReader(calls.clone())));
    assert_eq!(stream.try_next().await.err().unwrap().kind(), ErrorKind::Io);
    assert!(stream.next().await.is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use open_arrow_access_core::{Capability, Error, ErrorKind, Reader, Result, async_trait};

/// Inject an I/O error on every poll and count calls, including unintended retries.
pub(crate) struct FailingReader {
    calls: Arc<AtomicUsize>,
}

impl FailingReader {
    /// Keep the caller's counter observable after a stream takes ownership of the reader.
    pub(crate) fn new(calls: Arc<AtomicUsize>) -> Self {
        Self { calls }
    }
}

#[async_trait]
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

    /// Expose unit metadata for a reader used only to test failure handling.
    fn metadata(&self) -> &() {
        &()
    }

    /// Record a poll and fail so tests can detect consumers polling after an error.
    async fn next(&mut self) -> Result<Option<u8>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(Error::new(ErrorKind::Io, "test_read", "injected failure"))
    }
}

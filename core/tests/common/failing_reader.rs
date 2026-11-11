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
    pub(crate) fn new(calls: Arc<AtomicUsize>) -> Self {
        Self { calls }
    }
}

#[async_trait]
impl Reader for FailingReader {
    type Item = u8;
    type Metadata = ();
    type Capability = Capability;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    fn metadata(&self) -> &() {
        &()
    }

    async fn next(&mut self) -> Result<Option<u8>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(Error::new(ErrorKind::Io, "test_read", "injected failure"))
    }
}

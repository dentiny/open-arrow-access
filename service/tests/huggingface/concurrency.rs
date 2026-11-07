use std::{
    num::NonZeroUsize,
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
    time::Duration,
};

use futures::TryStreamExt;
use http::{Request, Response};
use open_arrow_access_core::{Accessor, ByteReadOptions, ByteReadRequest, ByteReader, Bytes};
use opendal::{Buffer, HttpBody, HttpTransport};
use tokio::sync::{Notify, Semaphore};

use super::super::HuggingFaceAccessor;
use super::support::{MockHub, request_range};

#[derive(Default)]
struct InFlight {
    active: AtomicUsize,
    peak: AtomicUsize,
    cancelled: AtomicUsize,
    changed: Notify,
}

impl InFlight {
    async fn wait_for(&self, condition: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let changed = self.changed.notified();
                if condition() {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("requests did not reach the expected state");
    }
}

struct ActiveRequest {
    state: Arc<InFlight>,
    completed: bool,
}

impl Drop for ActiveRequest {
    fn drop(&mut self) {
        self.state.active.fetch_sub(1, SeqCst);
        if !self.completed {
            self.state.cancelled.fetch_add(1, SeqCst);
        }
        self.state.changed.notify_one();
    }
}

#[derive(Clone)]
struct GatedHub {
    hub: MockHub,
    blocked: Range<u64>,
    release: Arc<Semaphore>,
    state: Arc<InFlight>,
}

impl GatedHub {
    fn new(blocked: Range<u64>) -> Self {
        Self {
            hub: MockHub::file("bytes.bin", Bytes::from((0..64).collect::<Vec<u8>>())),
            blocked,
            release: Arc::new(Semaphore::new(0)),
            state: Arc::default(),
        }
    }

    async fn accessor(&self) -> HuggingFaceAccessor {
        MockHub::accessor_with_transport(self.clone())
            .await
            .with_chunk_size(NonZeroUsize::MIN)
    }
}

impl HttpTransport for GatedHub {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        let Some(range) = request.headers().get("range") else {
            return self.hub.fetch(request).await;
        };
        let start = request_range(range.to_str().unwrap(), 64).start as u64;
        let active = self.state.active.fetch_add(1, SeqCst) + 1;
        self.state.peak.fetch_max(active, SeqCst);
        let mut guard = ActiveRequest {
            state: self.state.clone(),
            completed: false,
        };
        self.state.changed.notify_one();
        if self.blocked.contains(&start) {
            self.release.acquire().await.unwrap().forget();
        }
        let response = self.hub.fetch(request).await;
        guard.completed = true;
        response
    }
}

async fn open_reader(accessor: &HuggingFaceAccessor) -> Box<dyn ByteReader> {
    accessor
        .open(
            ByteReadRequest::new("bytes.bin").with_options(ByteReadOptions {
                io_chunk_size: NonZeroUsize::new(4),
                io_concurrency: NonZeroUsize::new(4).unwrap(),
            }),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn dropping_a_sequential_stream_cancels_in_flight_requests() {
    let hub = GatedHub::new(4..16);
    let accessor = hub.accessor().await;
    let mut reader = open_reader(&accessor).await;
    assert_eq!(hub.hub.request_count(), 1, "opening only inspects metadata");
    assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), &[0]);
    hub.state
        .wait_for(|| hub.state.active.load(SeqCst) == 3)
        .await;
    drop(reader);
    hub.state
        .wait_for(|| hub.state.cancelled.load(SeqCst) == 3)
        .await;
    assert_eq!(hub.state.active.load(SeqCst), 0);
}

#[tokio::test]
async fn cancelling_interleaved_reads_releases_the_shared_budget() {
    let hub = GatedHub::new(4..32);
    let accessor = hub.accessor().await;
    let mut reader = open_reader(&accessor).await;
    reader.next().await.unwrap();
    hub.state
        .wait_for(|| hub.state.active.load(SeqCst) == 3)
        .await;
    // One range request runs alongside three prefetched requests. The other
    // range tasks wait for permits; cancellation must not strand the reader.
    let task = tokio::spawn(async move { reader.read_range(16..32).await });
    hub.state
        .wait_for(|| hub.state.active.load(SeqCst) == 4)
        .await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    hub.state
        .wait_for(|| hub.state.cancelled.load(SeqCst) == 4)
        .await;
    assert_eq!(hub.state.active.load(SeqCst), 0);
}

#[tokio::test]
async fn interleaved_ranges_share_the_reader_budget_and_preserve_the_cursor() {
    for bulk in [false, true] {
        let hub = GatedHub::new(4..16);
        let accessor = hub.accessor().await;
        let mut reader = open_reader(&accessor).await;
        assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), &[0]);
        hub.state
            .wait_for(|| hub.state.active.load(SeqCst) == 3)
            .await;
        tokio::time::timeout(Duration::from_secs(5), async {
            if bulk {
                let ranges = reader.read_ranges(vec![24..32, 16..24]).await.unwrap();
                assert_eq!(ranges[0].as_ref(), &(24..32).collect::<Vec<u8>>());
                assert_eq!(ranges[1].as_ref(), &(16..24).collect::<Vec<u8>>());
            } else {
                assert_eq!(
                    reader.read_range(16..32).await.unwrap().as_ref(),
                    &(16..32).collect::<Vec<u8>>()
                );
            }
        })
        .await
        .expect("range reads must progress alongside prefetch");
        assert_eq!(hub.state.peak.load(SeqCst), 4);

        // A separately opened reader gets its own budget.
        let mut independent = open_reader(&accessor).await;
        tokio::time::timeout(Duration::from_secs(5), independent.read_range(16..32))
            .await
            .expect("separate readers must have independent budgets")
            .unwrap();
        assert!(hub.state.peak.load(SeqCst) > 4);

        hub.release.add_permits(3);
        let chunks = open_arrow_access_core::into_stream(reader)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(chunks.concat(), (1..64).collect::<Vec<u8>>());
        assert_eq!(hub.state.cancelled.load(SeqCst), 0);
        assert_eq!(
            hub.hub
                .captured()
                .iter()
                .filter(|r| r.range.is_some())
                .count(),
            24,
            "sixteen sequential chunks and four ranges per reader, without rereads"
        );
    }
}

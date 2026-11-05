use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use open_arrow_access_core::{Accessor, ByteReadOptions, ByteReadRequest};
use opendal::{
    Execute, Executor, OperationContext, Operator,
    raw::{BoxedStaticFuture, Layer, Servicer},
    services::Fs,
};

use crate::backend::OpenDalAccessor;

#[derive(Debug, Default)]
struct TaskCounts {
    active: AtomicUsize,
    peak: AtomicUsize,
}

struct ActiveTask(Arc<TaskCounts>);

impl Drop for ActiveTask {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

struct CountingExecutor {
    inner: Arc<dyn Execute>,
    counts: Arc<TaskCounts>,
}

impl Execute for CountingExecutor {
    fn execute(&self, future: BoxedStaticFuture<()>) {
        let counts = self.counts.clone();
        self.inner.execute(Box::pin(async move {
            let active = counts.active.fetch_add(1, Ordering::SeqCst) + 1;
            counts.peak.fetch_max(active, Ordering::SeqCst);
            let _guard = ActiveTask(counts);
            // Force overlap without relying on filesystem speed or a timed sleep.
            tokio::task::yield_now().await;
            future.await;
        }));
    }
}

#[derive(Debug)]
struct CountTasksLayer(Arc<TaskCounts>);

impl Layer for CountTasksLayer {
    fn apply_context(&self, _: Servicer, inner: OperationContext) -> OperationContext {
        inner.with_executor(Executor::with(CountingExecutor {
            inner: inner.executor().clone().into_inner(),
            counts: self.0.clone(),
        }))
    }
}

#[tokio::test]
async fn io_options_enable_bounded_backend_concurrency_for_single_and_bulk_reads() {
    let dir = tempfile::tempdir().unwrap();
    let contents = (0..64).collect::<Vec<u8>>();
    std::fs::write(dir.path().join("data.bin"), &contents).unwrap();
    for concurrency in [1, 4] {
        let counts = Arc::new(TaskCounts::default());
        let operator = Operator::new(Fs::default().root(dir.path().to_str().unwrap()))
            .unwrap()
            .layer(CountTasksLayer(counts.clone()));
        let accessor = OpenDalAccessor::new(operator);
        let mut reader = accessor
            .open(
                ByteReadRequest::new("data.bin").with_options(ByteReadOptions {
                    io_concurrency: std::num::NonZeroUsize::new(concurrency).unwrap(),
                    io_chunk_size: std::num::NonZeroUsize::new(8),
                }),
            )
            .await
            .unwrap();
        assert_eq!(reader.read_range(0..64).await.unwrap().as_ref(), contents);
        let ranges = reader.read_ranges(vec![32..64, 0..16]).await.unwrap();
        assert_eq!(ranges[0].as_ref(), &contents[32..64]);
        assert_eq!(ranges[1].as_ref(), &contents[..16]);
        let peak = counts.peak.load(Ordering::SeqCst);
        assert!(
            peak <= concurrency,
            "peak {peak} exceeds limit {concurrency}"
        );
        if concurrency > 1 {
            assert!(peak > 1, "configured reads did not execute concurrently");
        }
    }
}

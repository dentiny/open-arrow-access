use arrow_array::Int32Array;
use arrow_schema::{DataType, Field, Schema};
use futures::{StreamExt, stream};
use open_arrow_access_core::*;
use open_arrow_access_layers::ParallelReadLayer;
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::{Notify, Semaphore};

fn metadata() -> RecordMetadata {
    RecordMetadata {
        schema: Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)])),
        source: None,
    }
}
fn batch(n: i32) -> RecordBatch {
    RecordBatch::try_new(metadata().schema, vec![Arc::new(Int32Array::from(vec![n]))]).unwrap()
}
fn value(batch: &RecordBatch) -> i32 {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .value(0)
}
struct Source;
#[async_trait]
impl Accessor for Source {
    type Request = ScanPlan;
    type Reader = dyn RecordReader;
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }
    async fn open(&self, _: ScanPlan) -> Result<Box<dyn RecordReader>> {
        unreachable!()
    }
}
#[async_trait]
impl ScanPlanner for Source {
    async fn plan_scan(&self, plan: ScanPlan) -> Result<ScanPlan> {
        Ok(plan)
    }
}
async fn open(parts: Vec<ScanPartition>, p: usize) -> Box<dyn RecordReader> {
    Source
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(p).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ))
        .open(ScanPlan::new(metadata(), parts))
        .await
        .unwrap()
}
fn part(values: Vec<i32>) -> ScanPartition {
    Box::pin(async move {
        Ok(Box::new(StreamReader::new(
            stream::iter(values.into_iter().map(|n| Ok(batch(n)))),
            metadata(),
        )) as Box<dyn RecordReader>)
    })
}
#[tokio::test]
async fn ordered_duplicates_empty_partitions_and_single_worker() {
    for p in [1, 3] {
        let mut r = open(
            vec![part(vec![]), part(vec![2, 3]), part(vec![2]), part(vec![4])],
            p,
        )
        .await;
        let mut values = vec![];
        while let Some(b) = r.next().await.unwrap() {
            values.push(value(&b));
        }
        assert_eq!(values, [2, 3, 2, 4]);
        assert!(r.next().await.unwrap().is_none());
    }
    assert!(open(vec![], 2).await.next().await.unwrap().is_none());
}

#[tokio::test]
async fn overlaps_without_replacing_completed_buffered_partitions() {
    let release = Arc::new(Notify::new());
    let ready = Arc::new(Semaphore::new(0));
    let opened = Arc::new(AtomicUsize::new(0));
    let mut parts: Vec<ScanPartition> = vec![];
    for n in 0..20 {
        let (release, ready, opened) = (release.clone(), ready.clone(), opened.clone());
        parts.push(Box::pin(async move {
            opened.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                release.notified().await;
            } else {
                ready.add_permits(1);
            }
            part(vec![n]).await
        }));
    }
    let mut r = open(parts, 3).await;
    let task = tokio::spawn(async move {
        let b = r.next().await.unwrap().unwrap();
        (b, r)
    });
    ready.acquire_many(2).await.unwrap().forget();
    assert_eq!(opened.load(Ordering::SeqCst), 3);
    release.notify_one();
    let (b, mut r) = task.await.unwrap();
    assert_eq!(value(&b), 0);
    let mut count = 1;
    while r.next().await.unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 20);
}

#[tokio::test]
async fn later_error_interrupts_blocked_first_partition() {
    for panic in [false, true] {
        let first: ScanPartition = Box::pin(std::future::pending());
        let second: ScanPartition = Box::pin(async move {
            assert!(!panic, "injected worker panic");
            Err(Error::new(ErrorKind::Io, "test", "later open failed"))
        });
        let mut r = open(vec![first, second], 2).await;
        assert!(r.next().await.is_err());
        assert!(r.next().await.unwrap().is_none());
    }
}

struct Guard(Arc<Semaphore>);
impl Drop for Guard {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
}
#[tokio::test]
async fn drop_cancels_partition_work() {
    let dropped = Arc::new(Semaphore::new(0));
    let started = Arc::new(Semaphore::new(0));
    let (d, s) = (dropped.clone(), started.clone());
    let blocked: ScanPartition = Box::pin(async move {
        let _guard = Guard(d);
        s.add_permits(1);
        std::future::pending().await
    });
    let mut r = open(vec![part(vec![1]), blocked], 2).await;
    r.next().await.unwrap();
    started.acquire().await.unwrap().forget();
    drop(r);
    dropped.acquire().await.unwrap().forget();
}

#[tokio::test]
async fn rejects_partition_metadata_and_batch_schema_mismatch() {
    let wrong = RecordMetadata {
        schema: Arc::new(Schema::empty()),
        source: None,
    };
    let wrong_batch = RecordBatch::new_empty(wrong.schema.clone());
    let p: ScanPartition = Box::pin(async move {
        Ok(Box::new(StreamReader::new(
            stream::iter([Ok(wrong_batch)]),
            metadata(),
        )) as Box<dyn RecordReader>)
    });
    assert!(open(vec![p], 1).await.next().await.is_err());
    let p: ScanPartition = Box::pin(async move {
        Ok(Box::new(StreamReader::new(
            stream::empty::<Result<RecordBatch>>(),
            wrong,
        )) as Box<dyn RecordReader>)
    });
    assert!(open(vec![p], 1).await.next().await.is_err());
}

#[tokio::test]
async fn full_later_queue_stops_decoding_before_next_batch() {
    let produced = Arc::new(Semaphore::new(0));
    let polls = Arc::new(AtomicUsize::new(0));
    let (signal, count) = (produced.clone(), polls.clone());
    let later: ScanPartition = Box::pin(async move {
        let input = stream::iter((0..50).map(|n| Ok(batch(n)))).inspect(move |_| {
            count.fetch_add(1, Ordering::SeqCst);
            signal.add_permits(1);
        });
        Ok(Box::new(StreamReader::new(input, metadata())) as Box<dyn RecordReader>)
    });
    let mut r = open(vec![Box::pin(std::future::pending()), later], 2).await;
    let task = tokio::spawn(async move { r.next().await });
    // On the current-thread runtime, the producer runs from this signal to
    // its next suspension (reserving the full queue) before this task resumes.
    produced.acquire().await.unwrap().forget();
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn pending_partition_discovery_does_not_block_ready_output() {
    let partitions = stream::iter([Ok(part(vec![7]))]).chain(stream::pending());
    let mut reader = Source
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ))
        .open(ScanPlan::from_stream(metadata(), partitions))
        .await
        .unwrap();
    assert_eq!(value(&reader.next().await.unwrap().unwrap()), 7);
}

#[tokio::test]
async fn discovery_errors_interrupt_a_blocked_partition() {
    let partitions = stream::iter([
        Ok(Box::pin(std::future::pending()) as ScanPartition),
        Err(Error::new(ErrorKind::Io, "discover", "injected failure")),
    ]);
    let mut reader = Source
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ))
        .open(ScanPlan::from_stream(metadata(), partitions))
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap_err().operation(), "discover");
    assert!(reader.next().await.unwrap().is_none());
}

use arrow_array::Int32Array;
use arrow_schema::{DataType, Field, Schema};
use futures::{StreamExt, stream};
use open_arrow_access_core::*;
use open_arrow_access_layers::{ParallelReadLayer, UnorderedEmissionLayer};
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
};
use tokio::sync::{Notify, Semaphore, oneshot};

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
struct Source(OrderRequirement);
#[async_trait]
impl Accessor for Source {
    type Request = ScanPlan;
    type Reader = dyn RecordReader;
    fn capability(&self) -> RecordCapability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
        .with_ordering(self.0)
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
    open_plan(ScanPlan::new(metadata(), parts), p, 1).await
}
async fn open_mode(
    parts: Vec<ScanPartition>,
    p: usize,
    emission: EmissionPolicy,
) -> Box<dyn RecordReader> {
    let mut plan = ScanPlan::new(metadata(), parts).with_ordering(OrderRequirement::Reorderable);
    plan.emission = emission;
    open_plan(plan, p, /*prefetch=*/ 1).await
}
async fn open_plan(plan: ScanPlan, p: usize, prefetch: usize) -> Box<dyn RecordReader> {
    Source(OrderRequirement::Reorderable)
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(p).unwrap(),
            NonZeroUsize::new(prefetch).unwrap(),
        ))
        .open(plan)
        .await
        .unwrap()
}
fn part(values: Vec<i32>) -> ScanPartition {
    Box::pin(async move {
        Ok(Box::new(StreamReader::records(
            stream::iter(values.into_iter().map(|n| Ok(batch(n)))),
            metadata(),
            OrderRequirement::Reorderable,
        )) as Box<dyn RecordReader>)
    })
}
// The start signal distinguishes cancelling a running worker from merely dropping
// an opener that has never been polled. The unsent sender observes resource release.
fn blocked_partition() -> (ScanPartition, oneshot::Receiver<()>, oneshot::Receiver<()>) {
    let (start, started) = oneshot::channel();
    let (lifetime, dropped) = oneshot::channel();
    let partition: ScanPartition = Box::pin(async move {
        let _lifetime = lifetime;
        start.send(()).unwrap();
        std::future::pending().await
    });
    (partition, started, dropped)
}

#[tokio::test]
async fn capability_controls_ordering_and_preserves_all_rows() {
    for ordering in [
        OrderRequirement::PreserveGlobal,
        OrderRequirement::Reorderable,
    ] {
        let source = Source(ordering).layer(ParallelReadLayer::new(
            /*partitions=*/ NonZeroUsize::new(3).unwrap(),
            /*prefetch=*/ NonZeroUsize::MIN,
        ));
        // The source capability must strengthen a permissive partition plan.
        let plan = ScanPlan::new(
            metadata(),
            vec![part(vec![0, 1, 1]), part(vec![]), part(vec![2, 3, 4])],
        )
        .with_ordering(OrderRequirement::Reorderable);
        let mut reader = if ordering == OrderRequirement::Reorderable {
            source.layer(UnorderedEmissionLayer).open(plan).await
        } else {
            source.open(plan).await
        }
        .unwrap();
        assert_eq!(reader.capability().ordering, ordering);

        let mut values = Vec::new();
        while let Some(batch) = reader.next().await.unwrap() {
            values.push(value(&batch));
        }
        // Global-order output is compared directly; unordered output is sorted
        // by the caller before checking values and duplicate multiplicity.
        if ordering == OrderRequirement::Reorderable {
            values.sort_unstable();
        }
        assert_eq!(values, [0, 1, 1, 2, 3, 4]);
        assert!(reader.next().await.unwrap().is_none());
    }
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
    for n in 0..4 {
        let (release, ready, opened) = (release.clone(), ready.clone(), opened.clone());
        parts.push(Box::pin(async move {
            opened.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                release.notified().await;
            }
            let batches = stream::iter([Ok(batch(n))]).chain(stream::poll_fn(move |_| {
                ready.add_permits(1);
                Poll::Ready(None)
            }));
            Ok(Box::new(StreamReader::records(
                batches,
                metadata(),
                OrderRequirement::Reorderable,
            )) as Box<dyn RecordReader>)
        }));
    }
    // Two slots let each later worker queue its batch and reach EOF.
    let mut r = open_plan(ScanPlan::new(metadata(), parts), 3, 2).await;
    assert_eq!(opened.load(Ordering::SeqCst), 0);
    let task = tokio::spawn(async move {
        let b = r.next().await.unwrap().unwrap();
        (b, r)
    });
    ready.acquire_many(2).await.unwrap().forget();
    assert_eq!(opened.load(Ordering::SeqCst), 3);
    release.notify_one();
    let (b, mut r) = task.await.unwrap();
    assert_eq!(value(&b), 0);
    let mut values = vec![value(&b)];
    while let Some(batch) = r.next().await.unwrap() {
        values.push(value(&batch));
    }
    assert_eq!(values, (0..4).collect::<Vec<_>>());
}

#[tokio::test]
async fn drop_cancels_partition_work() {
    for emission in [EmissionPolicy::Ordered, EmissionPolicy::Unordered] {
        let (blocked, started, dropped) = blocked_partition();
        let mut r = open_mode(vec![part(vec![1]), blocked], 2, emission).await;
        r.next().await.unwrap();
        started.await.unwrap();
        drop(r);
        assert!(dropped.await.is_err());
    }
}

#[tokio::test]
async fn rejects_partition_metadata_and_batch_schema_mismatch() {
    for emission in [EmissionPolicy::Ordered, EmissionPolicy::Unordered] {
        let wrong = RecordMetadata {
            schema: Arc::new(Schema::empty()),
            source: None,
        };
        let wrong_batch = RecordBatch::new_empty(wrong.schema.clone());
        let p: ScanPartition = Box::pin(async move {
            Ok(Box::new(StreamReader::records(
                stream::iter([Ok(wrong_batch)]),
                metadata(),
                OrderRequirement::Reorderable,
            )) as Box<dyn RecordReader>)
        });
        assert!(open_mode(vec![p], 1, emission).await.next().await.is_err());
        let p: ScanPartition = Box::pin(async move {
            Ok(Box::new(StreamReader::records(
                stream::empty::<Result<RecordBatch>>(),
                wrong,
                OrderRequirement::Reorderable,
            )) as Box<dyn RecordReader>)
        });
        assert!(open_mode(vec![p], 1, emission).await.next().await.is_err());
    }
}

#[tokio::test]
async fn full_later_queue_stops_decoding_before_next_batch() {
    let produced = Arc::new(Semaphore::new(0));
    let signal = produced.clone();
    let later: ScanPartition = Box::pin(async move {
        let input = stream::iter((0..50).map(|n| Ok(batch(n)))).inspect(move |_| {
            signal.add_permits(1);
        });
        Ok(Box::new(StreamReader::records(
            input,
            metadata(),
            OrderRequirement::Reorderable,
        )) as Box<dyn RecordReader>)
    });
    let mut r = open(vec![Box::pin(std::future::pending()), later], 2).await;
    let task = tokio::spawn(async move { r.next().await });
    // On the current-thread runtime, the producer runs from this signal to
    // its next suspension (reserving the full queue) before this task resumes.
    produced.acquire().await.unwrap().forget();
    assert_eq!(produced.available_permits(), 0);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn ready_batches_progress_despite_pending_discovery_or_partitions() {
    // Pending discovery must not stall either emission policy.
    for emission in [EmissionPolicy::Ordered, EmissionPolicy::Unordered] {
        let partitions = stream::iter([Ok(part(vec![7]))]).chain(stream::pending());
        let mut plan = ScanPlan::from_stream(metadata(), partitions)
            .with_ordering(OrderRequirement::Reorderable);
        plan.emission = emission;
        let mut reader = open_plan(plan, 2, 1).await;
        assert_eq!(value(&reader.next().await.unwrap().unwrap()), 7);
    }

    // Unordered emission also bypasses an earlier blocked partition.
    let (release, released) = oneshot::channel();
    let blocked: ScanPartition = Box::pin(async {
        released.await.unwrap();
        part(vec![0]).await
    });
    let mut reader = open_mode(
        vec![blocked, part(vec![1, 2]), part(vec![])],
        2,
        EmissionPolicy::Unordered,
    )
    .await;
    assert_eq!(value(&reader.next().await.unwrap().unwrap()), 1);
    assert_eq!(value(&reader.next().await.unwrap().unwrap()), 2);
    release.send(()).unwrap();
    assert_eq!(value(&reader.next().await.unwrap().unwrap()), 0);
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
// Adding the two layers in either order must select the same emission policy
// and share one parallel-read budget.
async fn emission_and_parallel_layers_collect_one_budget_in_either_order() {
    let budget = ParallelReadLayer::new(NonZeroUsize::new(2).unwrap(), NonZeroUsize::MIN);
    let a = Source(OrderRequirement::Reorderable)
        .layer(budget)
        .layer(UnorderedEmissionLayer);
    let b = Source(OrderRequirement::Reorderable)
        .layer(UnorderedEmissionLayer)
        .layer(budget);
    let empty = || ScanPlan::new(metadata(), vec![]).with_ordering(OrderRequirement::Reorderable);
    let left = a.plan_scan(empty()).await.unwrap();
    let right = b.plan_scan(empty()).await.unwrap();
    assert_eq!(left.parallel_read, right.parallel_read);
    assert_eq!(left.emission, EmissionPolicy::Unordered);
    assert_eq!(right.emission, EmissionPolicy::Unordered);
    for reverse in [false, true] {
        let blocked = Box::pin(std::future::pending()) as ScanPartition;
        let plan = ScanPlan::new(metadata(), vec![blocked, part(vec![7])])
            .with_ordering(OrderRequirement::Reorderable);
        let mut reader = if reverse {
            b.open(plan).await
        } else {
            a.open(plan).await
        }
        .unwrap();
        assert_eq!(value(&reader.next().await.unwrap().unwrap()), 7);
        drop(reader);
        let rejected = ScanPlan::new(metadata(), vec![]);
        let result = if reverse {
            b.open(rejected).await
        } else {
            a.open(rejected).await
        };
        assert_eq!(result.err().unwrap().kind(), ErrorKind::InvalidInput);
    }
    let conflict = Source(OrderRequirement::Reorderable)
        .layer(budget)
        .layer(ParallelReadLayer::new(NonZeroUsize::MIN, NonZeroUsize::MIN));
    assert_eq!(
        conflict.open(empty()).await.err().unwrap().kind(),
        ErrorKind::InvalidInput
    );
}

#[tokio::test]
async fn unordered_rejects_source_requirement_before_discovery() {
    for ordering in [
        OrderRequirement::PreserveGlobal,
        OrderRequirement::Reorderable,
    ] {
        for parallel in [false, true] {
            // A permissive plan cannot weaken the source capability, and a
            // restrictive plan cannot be weakened by a permissive capability.
            let partitions = stream::poll_fn(|_| -> Poll<Option<Result<ScanPartition>>> {
                panic!("rejected scans must not poll partition discovery")
            });
            let mut plan = ScanPlan::from_stream(metadata(), partitions).with_ordering(
                if ordering == OrderRequirement::PreserveGlobal {
                    OrderRequirement::Reorderable
                } else {
                    OrderRequirement::PreserveGlobal
                },
            );
            if parallel {
                plan.parallel_read = Some(ParallelReadOptions {
                    partitions: NonZeroUsize::MIN,
                    prefetch: NonZeroUsize::MIN,
                });
            }
            let error = Source(ordering)
                .layer(UnorderedEmissionLayer)
                .open(plan)
                .await
                .err()
                .unwrap();
            assert_eq!(error.kind(), ErrorKind::InvalidInput);
            assert_eq!(error.operation(), "unordered_emission");
        }
    }
}

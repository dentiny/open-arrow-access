use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, types::Int64Type};
use futures::{StreamExt, stream};
use open_arrow_access_core::{
    Accessor, AccessorExt, Error, ErrorKind, OrderRequirement, ReadOptions, ReadRequest,
    RecordCapability, RecordMetadata, RecordReader, Result, ScanPartition, ScanPlan, ScanPlanner,
    StreamReader, async_trait,
};
use open_arrow_access_layers::{LimitLayer, ParallelReadLayer, SamplingLayer};
use open_arrow_access_test_support::{DropProbe, MemoryRecordAccessor, record::collect_column};

fn input() -> MemoryRecordAccessor {
    let batch = RecordBatch::try_from_iter([(
        "id",
        Arc::new(Int64Array::from_iter_values(0..10)) as ArrayRef,
    )])
    .unwrap();
    MemoryRecordAccessor::new(vec![
        batch.slice(0, 0),
        batch.slice(0, 3),
        batch.slice(3, 4),
        batch.slice(7, 3),
    ])
}

#[tokio::test]
async fn limit_is_global_across_batches_and_resets_on_open() {
    for (limit, expected, polls) in [
        (0, 0, 0),
        (3, 3, 2),
        (5, 5, 3),
        (10, 10, 4),
        (u64::MAX, 10, 5),
    ] {
        let source = input();
        let reads = source.polls.clone();
        let records = source.layer(LimitLayer::new(limit));
        for iteration in 1..=2 {
            let reader = records.open(ReadRequest::new("input")).await.unwrap();
            assert_eq!(
                collect_column::<Int64Type>(reader, 0).await,
                (0..expected).collect::<Vec<_>>()
            );
            assert_eq!(reads.load(Ordering::SeqCst), iteration * polls);
        }
    }
}

#[tokio::test]
async fn limit_preserves_projected_metadata_and_zero_column_row_counts() {
    let source = input();
    let reader = source.open(ReadRequest::new("input")).await.unwrap();
    let metadata = reader.metadata().clone();
    let capability = reader.capability();
    let records = source.layer(LimitLayer::new(5));
    let mut reader = records.open(ReadRequest::new("input")).await.unwrap();
    assert_eq!(reader.metadata().schema, metadata.schema);
    assert_eq!(reader.metadata().source, metadata.source);
    assert_eq!(reader.capability(), capability);
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 0);
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 3);
    let last = reader.next().await.unwrap().unwrap();
    assert_eq!(last.num_rows(), 2);
    assert_eq!(last.schema(), metadata.schema);
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());

    let request = ReadRequest::new("input").with_options(ReadOptions {
        projection: Some(vec![]),
        ..Default::default()
    });
    let mut reader = records.open(request).await.unwrap();
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.num_columns(), 0);
        rows += batch.num_rows();
    }
    assert_eq!(rows, 5);
}

#[tokio::test]
async fn limit_after_sampling_caps_selected_rows() {
    let sampled = input().layer(SamplingLayer::try_new(0.5, 42).unwrap());
    let expected =
        collect_column::<Int64Type>(sampled.open(ReadRequest::new("input")).await.unwrap(), 0)
            .await;
    assert!(expected.len() > 2);
    let limited = sampled.layer(LimitLayer::new(2));
    assert_eq!(
        collect_column::<Int64Type>(limited.open(ReadRequest::new("input")).await.unwrap(), 0)
            .await,
        expected[..2]
    );
}

// A planned scan exercises the same merged stream used by directories and
// partitioned formats.
struct Planned;

#[async_trait]
impl Accessor for Planned {
    type Request = ScanPlan;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        RecordCapability::sequential(OrderRequirement::Reorderable)
    }

    async fn open(&self, _: Self::Request) -> Result<Box<Self::Reader>> {
        unreachable!("ParallelReadLayer opens the planned partitions")
    }
}

#[async_trait]
impl ScanPlanner for Planned {
    async fn plan_scan(&self, plan: Self::Request) -> Result<Self::Request> {
        Ok(plan)
    }
}

#[tokio::test]
async fn parallel_limit_is_global_across_partitions() {
    let metadata = RecordMetadata {
        schema: input().schema,
        source: None,
    };
    let batch =
        RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![0, 1, 2])) as ArrayRef)])
            .unwrap();
    let partition_metadata = metadata.clone();
    let first: ScanPartition = Box::pin(async move {
        let batches = stream::iter([Ok(batch)]);
        Ok(Box::new(StreamReader::records(
            batches,
            partition_metadata,
            OrderRequirement::Reorderable,
        )) as Box<dyn RecordReader>)
    });
    // A second partition must not get a fresh row budget.
    let second: ScanPartition = Box::pin(async { input().open(ReadRequest::new("second")).await });
    let records = Planned
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::MIN,
        ))
        .layer(LimitLayer::new(5));
    let reader = records
        .open(ScanPlan::new(metadata, vec![first, second]))
        .await
        .unwrap();
    assert_eq!(
        collect_column::<Int64Type>(reader, 0).await,
        [0, 1, 2, 0, 1]
    );
}

struct Watched(Arc<AtomicBool>);

#[async_trait]
impl Accessor for Watched {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        input().capability()
    }

    async fn open(&self, _: ReadRequest) -> Result<Box<Self::Reader>> {
        let batch = RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
        )])
        .unwrap();
        let metadata = RecordMetadata {
            schema: batch.schema(),
            source: None,
        };
        let probe = DropProbe::new(self.0.clone());
        let batches = stream::iter([
            Ok(batch),
            Err(Error::new(
                ErrorKind::PermissionDenied,
                "test",
                "injected error",
            )),
        ])
        .map(move |item| {
            let _ = &probe;
            item
        });
        Ok(Box::new(StreamReader::records(
            batches,
            metadata,
            self.capability().ordering,
        )))
    }
}

#[tokio::test]
async fn limit_releases_source_without_an_extra_next_and_errors_are_terminal() {
    for limit in [0, 2, 3, 4] {
        let dropped = Arc::new(AtomicBool::new(false));
        let source = Watched(dropped.clone()).layer(LimitLayer::new(limit));
        let mut reader = source.open(ReadRequest::new("input")).await.unwrap();
        if limit == 0 {
            assert!(dropped.load(Ordering::SeqCst));
        } else {
            assert!(!dropped.load(Ordering::SeqCst));
            assert_eq!(
                reader.next().await.unwrap().unwrap().num_rows(),
                limit.min(3) as usize
            );
            if limit > 3 {
                let error = reader.next().await.unwrap_err();
                assert_eq!(error.kind(), ErrorKind::PermissionDenied);
                assert_eq!(error.operation(), "test");
            }
            assert!(dropped.load(Ordering::SeqCst));
        }
        assert!(reader.next().await.unwrap().is_none());
        assert!(reader.next().await.unwrap().is_none());
    }
}

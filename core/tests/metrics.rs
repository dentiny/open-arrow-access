use std::{ops::Range, sync::Arc};

use arrow_array::Int32Array;
use arrow_schema::{DataType, Field, Schema};
use futures::{future, stream};
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteMetadata, ByteReadRequest, ByteReader, Bytes, Capability, ErrorKind,
    OrderRequirement, Reader, RecordBatch, RecordCapability, RecordMetadata, RecordReader, Result,
    ScanPlan, ScanPlanner, ScanStatistics, Statistic, StatisticsProvider, StreamReader,
    async_trait,
    metrics::{InMemoryExporter, MetricAggregate, MetricsExporter, MetricsLayer},
};
use open_arrow_access_test_support::{MemoryAccessor, collect_bytes};

fn counter(exporter: &InMemoryExporter, name: &'static str, operation: &'static str) -> u64 {
    match exporter.snapshot().get(&(name, operation)) {
        Some(MetricAggregate::Counter(n)) => *n,
        None => 0,
        other => panic!("unexpected counter: {other:?}"),
    }
}

#[tokio::test]
async fn byte_output_is_unchanged_and_clones_share_one_scope() {
    let metrics = MetricsLayer::default();
    let source = MemoryAccessor::new(b"abcdef", 2)
        .with_ranges()
        .with_listing();
    let capability = source.capability();
    let accessor = source.layer(metrics.clone());
    assert_eq!(accessor.capability(), capability);
    for accessor in [accessor.clone(), accessor] {
        let mut reader = accessor.open("data".into()).await.unwrap();
        assert_eq!(reader.metadata().content_length, Some(6));
        assert_eq!(reader.metadata().source.as_ref().unwrap().locator(), "data");
        assert_eq!(collect_bytes(reader.as_mut()).await.unwrap(), b"abcdef"[..]);
        assert!(reader.next().await.unwrap().is_none());
    }
    let exporter = metrics.exporter();
    assert_eq!(counter(exporter, "oaa_bytes_delivered_total", "next"), 12);
    assert_eq!(counter(exporter, "oaa_chunks_total", "next"), 6);
    assert_eq!(
        counter(exporter, "oaa_readers_completed_total", "reader"),
        2
    );
    assert_eq!(
        counter(exporter, "oaa_readers_dropped_early_total", "reader"),
        0
    );
    assert_eq!(
        exporter.snapshot()[&("oaa_executing", "next")],
        MetricAggregate::Gauge(0)
    );
    assert!(MetricsLayer::default().exporter().snapshot().is_empty());
}

#[tokio::test]
async fn failed_and_early_dropped_readers_keep_partial_counts() {
    let metrics = MetricsLayer::default();
    let accessor = MemoryAccessor::new(b"abcdef", 2)
        .with_failure(2, ErrorKind::Io)
        .layer(metrics.clone());
    let mut reader = accessor.open("data".into()).await.unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap(), "ab");
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Io);
    assert!(reader.next().await.unwrap().is_none());
    drop(reader);
    let accessor = MemoryAccessor::new(b"abcdef", 2).layer(metrics.clone());
    let mut reader = accessor.open("data".into()).await.unwrap();
    reader.next().await.unwrap();
    drop(reader);
    let exporter = metrics.exporter();
    assert_eq!(counter(exporter, "oaa_bytes_delivered_total", "next"), 4);
    assert_eq!(counter(exporter, "oaa_readers_failed_total", "reader"), 1);
    assert_eq!(
        counter(exporter, "oaa_readers_dropped_early_total", "reader"),
        1
    );
    assert_eq!(counter(exporter, "oaa_errors_total", "next"), 1);
    let accessor = MemoryAccessor::new(b"", 1)
        .with_open_failure(ErrorKind::Io)
        .layer(metrics.clone());
    assert!(accessor.open("missing".into()).await.is_err());
    assert_eq!(counter(exporter, "oaa_errors_total", "open"), 1);
    assert_eq!(counter(exporter, "oaa_readers_opened_total", "reader"), 2);
}

struct BulkAccessor;
struct BulkReader {
    metadata: ByteMetadata,
}
#[async_trait]
impl Accessor for BulkAccessor {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: true,
            ..Default::default()
        }
    }
    async fn open(&self, _: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        Ok(Box::new(BulkReader {
            metadata: ByteMetadata::default(),
        }))
    }
}
#[async_trait]
impl Reader for BulkReader {
    type Item = Bytes;
    type Metadata = ByteMetadata;
    type Capability = Capability;
    fn capability(&self) -> Capability {
        BulkAccessor.capability()
    }
    fn metadata(&self) -> &ByteMetadata {
        &self.metadata
    }
    async fn next(&mut self) -> Result<Option<Bytes>> {
        Ok(None)
    }
}
#[async_trait]
impl ByteReader for BulkReader {
    async fn read_range(&mut self, _: Range<u64>) -> Result<Bytes> {
        panic!("batch range optimization was lost")
    }
    async fn read_ranges(&mut self, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        Ok(ranges
            .into_iter()
            .map(|range| Bytes::from(vec![42; (range.end - range.start) as usize]))
            .collect())
    }
}

#[tokio::test]
async fn bulk_ranges_are_delegated_once_and_count_only_returned_bytes() {
    let metrics = MetricsLayer::default();
    let accessor = BulkAccessor.layer(metrics.clone());
    let mut reader = accessor.open("data".into()).await.unwrap();
    let ranges = vec![0..4, 2..6, 6..6];
    let bytes = reader.read_ranges(ranges).await.unwrap();
    assert_eq!(bytes.iter().map(Bytes::len).collect::<Vec<_>>(), [4, 4, 0]);
    assert!(reader.read_ranges(vec![]).await.unwrap().is_empty());
    let exporter = metrics.exporter();
    assert_eq!(
        counter(exporter, "oaa_bytes_delivered_total", "read_ranges"),
        8
    );
    assert_eq!(counter(exporter, "oaa_calls_total", "read_ranges"), 2);
    assert_eq!(counter(exporter, "oaa_calls_total", "read_range"), 0);
    let ranged = MemoryAccessor::new(b"abcdef", 2)
        .with_ranges()
        .layer(metrics.clone());
    let mut reader = ranged.open("data".into()).await.unwrap();
    assert_eq!(reader.read_range(1..4).await.unwrap(), "bcd");
    assert!(reader.read_range(7..8).await.is_err());
    assert_eq!(
        counter(exporter, "oaa_bytes_delivered_total", "read_range"),
        3
    );
    assert_eq!(counter(exporter, "oaa_errors_total", "read_range"), 1);
}

struct PendingAccessor;
#[async_trait]
impl Accessor for PendingAccessor {
    type Request = bool;
    type Reader = dyn ByteReader;
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            ..Default::default()
        }
    }
    async fn open(&self, pending: bool) -> Result<Box<dyn ByteReader>> {
        if pending {
            return future::pending().await;
        }
        Ok(Box::new(StreamReader::new(
            stream::pending::<Result<Bytes>>(),
            ByteMetadata::default(),
        )))
    }
}

#[tokio::test]
async fn cancellation_balances_gauges_without_creating_errors_or_output() {
    let metrics = MetricsLayer::default();
    let accessor = PendingAccessor.layer(metrics.clone());
    let mut open = Box::pin(accessor.open(true));
    assert!(futures::poll!(&mut open).is_pending());
    assert_eq!(
        metrics.exporter().snapshot()[&("oaa_executing", "open")],
        MetricAggregate::Gauge(1)
    );
    drop(open);
    let mut reader = accessor.open(false).await.unwrap();
    let mut next = Box::pin(reader.next());
    assert!(futures::poll!(&mut next).is_pending());
    drop(next);
    drop(reader);
    let exporter = metrics.exporter();
    for operation in ["open", "next"] {
        assert_eq!(counter(exporter, "oaa_cancelled_total", operation), 1);
        assert_eq!(counter(exporter, "oaa_errors_total", operation), 0);
        assert_eq!(
            exporter.snapshot()[&("oaa_executing", operation)],
            MetricAggregate::Gauge(0)
        );
    }
    assert_eq!(counter(exporter, "oaa_bytes_delivered_total", "next"), 0);
    assert_eq!(
        counter(exporter, "oaa_readers_dropped_early_total", "reader"),
        1
    );
}

struct Records;
impl Records {
    fn metadata() -> RecordMetadata {
        RecordMetadata {
            schema: Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)])),
            source: None,
        }
    }
}
#[async_trait]
impl Accessor for Records {
    type Request = ();
    type Reader = dyn RecordReader;
    fn capability(&self) -> RecordCapability {
        RecordCapability::sequential(OrderRequirement::PreserveGlobal)
    }
    async fn open(&self, _: ()) -> Result<Box<dyn RecordReader>> {
        let metadata = Self::metadata();
        let batch = RecordBatch::try_new(
            metadata.schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        Ok(Box::new(StreamReader::records(
            stream::iter([Ok(batch)]),
            metadata,
            OrderRequirement::PreserveGlobal,
        )))
    }
}
#[async_trait]
impl StatisticsProvider for Records {
    async fn statistics(&self, _: ()) -> Result<ScanStatistics> {
        Ok(ScanStatistics {
            row_count: Statistic::Exact(3),
            source_bytes: Statistic::Unknown,
        })
    }
}
#[async_trait]
impl ScanPlanner for Records {
    async fn plan_scan(&self, _: ()) -> Result<ScanPlan> {
        Ok(ScanPlan::new(
            Self::metadata(),
            vec![Box::pin(async { Records.open(()).await })],
        ))
    }
}

#[tokio::test]
async fn record_output_statistics_and_lazy_scan_planning_are_preserved() {
    let metrics = MetricsLayer::default();
    let accessor = Records.layer(metrics.clone());
    assert_eq!(
        accessor.statistics(()).await.unwrap().row_count,
        Statistic::Exact(3)
    );
    let plan = accessor.plan_scan(()).await.unwrap();
    assert_eq!(
        counter(metrics.exporter(), "oaa_readers_opened_total", "reader"),
        0
    );
    assert_eq!(plan.ordering, OrderRequirement::PreserveGlobal);
    let mut planned = plan.into_reader();
    let mut direct = accessor.open(()).await.unwrap();
    assert_eq!(direct.metadata().schema, planned.metadata().schema);
    assert_eq!(direct.next().await.unwrap(), planned.next().await.unwrap());
    assert!(direct.next().await.unwrap().is_none());
    assert!(planned.next().await.unwrap().is_none());
    let exporter = metrics.exporter();
    assert_eq!(counter(exporter, "oaa_rows_total", "next"), 6);
    assert_eq!(counter(exporter, "oaa_batches_total", "next"), 2);
    assert_eq!(
        counter(exporter, "oaa_readers_completed_total", "reader"),
        2
    );
    assert_eq!(counter(exporter, "oaa_bytes_delivered_total", "next"), 0);
}

#[test]
fn exporter_aggregates_observations_without_retaining_samples() {
    let exporter = InMemoryExporter::default();
    for value in [0.5, 1.5] {
        exporter.record_observation("duration", "read", value);
    }
    let snapshot = exporter.snapshot();
    assert_eq!(snapshot.len(), 1);
    match snapshot[&("duration", "read")] {
        MetricAggregate::Summary(summary) => {
            assert_eq!(summary.count, 2);
            assert_eq!(summary.sum, 2.0);
            assert_eq!(summary.max, 1.5);
        }
        other => panic!("unexpected: {other:?}"),
    }
}

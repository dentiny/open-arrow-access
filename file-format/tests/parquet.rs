use open_arrow_access_file_format::{
    FileAccessor,
    parquet::{ParquetAccessor, ParquetFormat},
};
use open_arrow_access_storage::OpenDalAccessor;
#[path = "parquet/support.rs"]
mod support;

use std::{
    error::Error as _,
    num::NonZeroUsize,
    sync::{Arc, Mutex, atomic::Ordering},
};

use arrow_array::{StringArray, types::Int64Type};
use futures::{StreamExt, TryStreamExt};
use more_asserts::{assert_gt, assert_le};
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteAccessor, ByteReadOptions, Capability, ErrorKind, Layer,
    ListRequest, ObjectRef, ObjectStream, ReadOptions, ReadRequest, RecordBatch, RecordReader,
    Result, ScanPlanner, async_trait, into_stream,
};
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_test_support::format;
use parquet::basic::Compression;
use support::{
    ByteObserverLayer, MemoryAccessor, Observations, SequentialLayer, fixture, fixture_path, ids,
};

#[tokio::test]
async fn local_parquet_preserves_values_nulls_and_order_across_row_groups() {
    for compression in [Compression::UNCOMPRESSED, Compression::SNAPPY] {
        let fixture = fixture(compression).await;
        let accessor = FileAccessor::from_format(
            OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(
                fixture.dir.path(),
            )),
            ParquetFormat,
        );
        let mut reader = accessor
            .open(ReadRequest::new("data.parquet").with_options(ReadOptions {
                batch_size: NonZeroUsize::new(2).unwrap(),
                projection: None,
            }))
            .await
            .unwrap();
        assert_eq!(reader.metadata().schema.as_ref(), fixture.schema.as_ref());
        let mut actual_ids = Vec::new();
        let mut titles = Vec::new();
        while let Some(batch) = reader.next().await.unwrap() {
            assert_le!(batch.num_rows(), 2);
            assert_eq!(batch.schema(), reader.metadata().schema);
            actual_ids.extend(ids(&batch));
            titles.extend(
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|s| s.map(str::to_owned)),
            );
        }
        assert_eq!(actual_ids, (0..7).collect::<Vec<_>>());
        assert_eq!(
            titles,
            vec![
                Some("zero".into()),
                None,
                Some("two".into()),
                Some("three".into()),
                None,
                Some("five".into()),
                Some("six".into())
            ]
        );
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn projection_honors_requested_field_order_and_schema() {
    let fixture = fixture(Compression::UNCOMPRESSED).await;
    let accessor = ParquetAccessor::new(OpenDalAccessor::new(
        open_arrow_access_test_support::fs_operator(fixture.dir.path()),
    ));
    let batches = format::assert_projection(
        &accessor,
        ReadRequest::new("data.parquet").with_options(ReadOptions {
            projection: Some(vec!["title".into(), "id".into()]),
            ..ReadOptions::default()
        }),
        &fixture.schema,
        7,
    )
    .await;
    let values = open_arrow_access_test_support::record::collect_values::<Int64Type>(&batches, 1);
    assert_eq!(values, (0..7).collect::<Vec<_>>());
}

#[tokio::test]
async fn zero_column_projection_preserves_row_count() {
    let fixture = fixture(Compression::UNCOMPRESSED).await;
    let accessor = ParquetAccessor::new(OpenDalAccessor::new(
        open_arrow_access_test_support::fs_operator(fixture.dir.path()),
    ));
    format::assert_projection(
        &accessor,
        ReadRequest::new("data.parquet").with_options(ReadOptions {
            projection: Some(vec![]),
            ..ReadOptions::default()
        }),
        &fixture.schema,
        7,
    )
    .await;
}

#[tokio::test]
async fn projection_rejects_unknown_and_duplicate_fields() {
    let fixture = fixture(Compression::UNCOMPRESSED).await;
    let accessor = ParquetAccessor::new(OpenDalAccessor::new(
        open_arrow_access_test_support::fs_operator(fixture.dir.path()),
    ));
    for columns in [vec!["missing".into()], vec!["id".into(), "id".into()]] {
        let result = accessor
            .open(ReadRequest::new("data.parquet").with_options(ReadOptions {
                projection: Some(columns),
                ..ReadOptions::default()
            }))
            .await;
        assert_eq!(result.err().unwrap().kind(), ErrorKind::InvalidInput);
    }
}

#[tokio::test]
async fn malformed_parquet_returns_contextual_decode_error() {
    let accessor = ParquetAccessor::new(OpenDalAccessor::new(
        open_arrow_access_test_support::fs_operator(fixture_path("")),
    ));
    let error = accessor
        .open(ReadRequest::new("bad-magic.parquet"))
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::Decode);
    assert_eq!(error.operation(), "inspect_parquet");
    assert!(error.source().is_some());
}

#[tokio::test]
async fn dropping_reader_does_not_start_background_reads_and_errors_are_terminal() {
    let fixture = fixture(Compression::UNCOMPRESSED).await;
    let observations = Arc::new(Observations::default());
    let byte_accessor = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(
        fixture.dir.path(),
    ))
    .layer(ByteObserverLayer(observations.clone()));
    let accessor = ParquetAccessor::new(byte_accessor);
    let mut reader = accessor
        .open(ReadRequest::new("data.parquet"))
        .await
        .unwrap();
    let before = observations
        .range_reads
        .load(std::sync::atomic::Ordering::SeqCst);
    tokio::task::yield_now().await;
    assert_eq!(
        observations
            .range_reads
            .load(std::sync::atomic::Ordering::SeqCst),
        before
    );
    observations
        .fail_reads
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let error = reader.next().await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::Io);
    assert!(reader.next().await.unwrap().is_none());
    drop(reader);
    assert_eq!(
        observations.drops.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}

struct OpenObserverLayer {
    label: &'static str,
    events: Arc<Mutex<Vec<&'static str>>>,
}

struct OpenObserverAccessor<A> {
    inner: A,
    label: &'static str,
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl<A: Accessor> Layer<A> for OpenObserverLayer {
    type Layered = OpenObserverAccessor<A>;

    fn layer(&self, inner: A) -> Self::Layered {
        OpenObserverAccessor {
            inner,
            label: self.label,
            events: self.events.clone(),
        }
    }
}

#[async_trait]
impl<A: Accessor> Accessor for OpenObserverAccessor<A> {
    type Request = A::Request;
    type Reader = A::Reader;

    fn capability(&self) -> Capability {
        let inner = self.inner.capability();
        Capability {
            read: inner.read,
            read_with_range: inner.read_with_range,
            read_parallel: inner.read_parallel,
            list: inner.list,
        }
    }

    async fn open(&self, request: Self::Request) -> Result<Box<Self::Reader>> {
        self.events.lock().unwrap().push(self.label);
        self.inner.open(request).await
    }
}

#[async_trait]
impl<A: ByteAccessor> ByteAccessor for OpenObserverAccessor<A> {
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        self.inner.list(request).await
    }
}

#[tokio::test]
async fn one_layer_contract_composes_at_byte_and_record_boundaries_in_known_order() {
    let fixture = fixture(Compression::UNCOMPRESSED).await;
    let events = Arc::new(Mutex::new(Vec::new()));
    let layer = |label| OpenObserverLayer {
        label,
        events: events.clone(),
    };
    let bytes = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(
        fixture.dir.path(),
    ))
    .layer(layer("inner-byte"))
    .layer(layer("outer-byte"));
    assert!(bytes.capability().list);
    assert_eq!(
        bytes
            .list("".into())
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap(),
        vec![ObjectRef::new("data.parquet")]
    );
    let bytes: Arc<dyn ByteAccessor> = Arc::new(bytes);
    assert!(bytes.capability().read_with_range);
    let records = ParquetAccessor::new(bytes).layer(layer("record"));
    assert!(records.capability().read);
    assert!(!records.capability().read_with_range);
    let reader = records
        .open(ReadRequest::new("data.parquet"))
        .await
        .unwrap();
    assert!(reader.capability().read);
    assert!(!reader.capability().read_with_range);
    let batches = into_stream(reader).try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(
        batches.iter().flat_map(ids).collect::<Vec<_>>(),
        (0..7).collect::<Vec<_>>()
    );
    assert_eq!(
        *events.lock().unwrap(),
        vec!["record", "outer-byte", "inner-byte"]
    );
}

#[tokio::test]
async fn capability_downgrade_matches_operations_and_blocks_parquet() {
    let fixture = fixture(Compression::UNCOMPRESSED).await;
    let fs = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(
        fixture.dir.path(),
    ));
    assert!(fs.capability().read_with_range);
    let bytes = fs.layer(SequentialLayer);
    assert!(bytes.capability().read);
    assert!(!bytes.capability().read_with_range);
    let mut reader = bytes.open("data.parquet".into()).await.unwrap();
    assert!(!reader.capability().read_with_range);
    assert_eq!(
        reader.read_range(0..4).await.err().unwrap().kind(),
        ErrorKind::Unsupported
    );
    assert!(!reader.next().await.unwrap().unwrap().is_empty());

    let records = ParquetAccessor::new(bytes);
    assert!(!records.capability().read);
    assert_eq!(
        records
            .open(ReadRequest::new("data.parquet"))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Unsupported
    );
}

#[tokio::test]
async fn parquet_forwards_io_options_and_bulk_ranges_through_third_party_layers() {
    let data = tokio::fs::read(fixture_path("ordered-uncompressed.parquet"))
        .await
        .unwrap();
    let observations = Arc::new(Observations::default());
    let bytes = MemoryAccessor::new(&data, usize::MAX)
        .with_ranges()
        .layer(ByteObserverLayer(observations.clone()));
    let records = ParquetAccessor::new(bytes);
    for concurrency in [1, 3] {
        let options = ByteReadOptions {
            io_concurrency: NonZeroUsize::new(concurrency).unwrap(),
            io_chunk_size: NonZeroUsize::new(64),
        };
        let reader = records
            .open(ReadRequest::new("opaque:fixture").with_byte_options(options))
            .await
            .unwrap();
        let batches = into_stream(reader).try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(
            batches.iter().flat_map(ids).collect::<Vec<_>>(),
            (0..7).collect::<Vec<_>>()
        );
        assert_eq!(
            *observations.byte_options.lock().unwrap().last().unwrap(),
            options
        );
    }
    assert_gt!(
        observations
            .batch_reads
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_gt!(
        observations
            .max_batch_ranges
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}

#[tokio::test]
async fn parquet_preserves_source_error_categories_during_inspection_and_decoding() {
    for kind in [ErrorKind::Io, ErrorKind::PermissionDenied] {
        for inspect in [true, false] {
            let fixture = fixture(Compression::UNCOMPRESSED).await;
            let observations = Arc::new(Observations::default());
            *observations.failure_kind.lock().unwrap() = Some(kind);
            observations.fail_reads.store(inspect, Ordering::SeqCst);
            let bytes = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(
                fixture.dir.path(),
            ))
            .layer(ByteObserverLayer(observations.clone()));
            let accessor = ParquetAccessor::new(bytes);
            let error = if inspect {
                accessor
                    .open(ReadRequest::new("data.parquet"))
                    .await
                    .err()
                    .unwrap()
            } else {
                let mut reader = accessor
                    .open(ReadRequest::new("data.parquet"))
                    .await
                    .unwrap();
                observations.fail_reads.store(true, Ordering::SeqCst);
                let error = reader.next().await.unwrap_err();
                let calls = observations.range_reads.load(Ordering::SeqCst);
                assert!(reader.next().await.unwrap().is_none());
                assert!(reader.next().await.unwrap().is_none());
                assert_eq!(observations.range_reads.load(Ordering::SeqCst), calls);
                error
            };
            assert_eq!(error.kind(), kind, "inspect={inspect}");
            assert_eq!(
                error.operation(),
                if inspect {
                    "inspect_parquet"
                } else {
                    "decode_parquet"
                }
            );
            assert_eq!(ErrorKind::from_source(error.source().unwrap()), Some(kind));
        }
    }
}

async fn rows(mut reader: Box<dyn RecordReader>) -> Vec<RecordBatch> {
    let mut rows = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        rows.extend((0..batch.num_rows()).map(|row| batch.slice(row, 1)));
    }
    rows
}

#[tokio::test]
async fn row_group_plan_reuses_metadata_and_preserves_projection_and_rows() {
    let fixture = fixture(Compression::SNAPPY).await;
    let observations = Arc::new(Observations::default());
    let bytes = Arc::new(
        OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(
            fixture.dir.path(),
        ))
        .layer(ByteObserverLayer(observations.clone())),
    );
    let accessor = ParquetAccessor::new(bytes);
    let mut plan = accessor
        .plan_scan(ReadRequest::new("data.parquet"))
        .await
        .unwrap();
    assert!(plan.partitions.size_hint().0 > 1);
    assert_eq!(observations.byte_options.lock().unwrap().len(), 1);
    let reads = observations.range_reads.load(Ordering::SeqCst);
    while let Some(partition) = plan.partitions.next().await {
        let reader = partition.unwrap().await.unwrap();
        assert_eq!(reader.metadata().schema, fixture.schema);
    }
    assert_eq!(observations.range_reads.load(Ordering::SeqCst), reads);
    for projection in [None, Some(vec!["title".into(), "id".into()]), Some(vec![])] {
        let request = ReadRequest::new("data.parquet").with_options(ReadOptions {
            batch_size: NonZeroUsize::new(2).unwrap(),
            projection,
        });
        let expected = rows(accessor.open(request.clone()).await.unwrap()).await;
        let parallel = accessor.clone().layer(ParallelReadLayer::new(
            NonZeroUsize::new(3).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ));
        let actual = rows(parallel.open(request).await.unwrap()).await;
        assert_eq!(actual, expected);
    }
}

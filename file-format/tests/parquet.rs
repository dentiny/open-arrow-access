use std::{
    error::Error as _,
    num::NonZeroUsize,
    ops::Range,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use arrow_array::{StringArray, types::Int64Type};
use arrow_schema::{DataType, Field, Schema};
use futures::{StreamExt, TryStreamExt};
use more_asserts::{assert_gt, assert_le};
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteAccessor, ByteMetadata, ByteReadOptions, ByteReadRequest,
    ByteReader, Bytes, Capability, Error, ErrorKind, Layer, ListRequest, ObjectRef, ObjectStream,
    ReadOptions, ReadRequest, Reader, RecordBatch, RecordReader, Result, ScanPlanner, SchemaRef,
    async_trait, into_stream,
};
use open_arrow_access_file_format::{
    FileAccessor,
    parquet::{ParquetAccessor, ParquetFormat},
};
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::{MemoryAccessor, format};
use parquet::basic::Compression;

struct Fixture {
    dir: tempfile::TempDir,
    schema: SchemaRef,
}

async fn fixture(compression: Compression) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("title", DataType::Utf8, true),
        ],
        [("fixture".into(), "ordered".into())].into(),
    ));
    let name = match compression {
        Compression::UNCOMPRESSED => "ordered-uncompressed.parquet",
        Compression::SNAPPY => "ordered-snappy.parquet",
        other => panic!("no checked-in Parquet fixture for {other:?}"),
    };
    tokio::fs::copy(fixture_path(name), dir.path().join("data.parquet"))
        .await
        .unwrap();
    Fixture { dir, schema }
}

fn fixture_path(name: &str) -> PathBuf {
    open_arrow_access_test_support::fixture_path!("parquet", name)
}

fn ids(batch: &RecordBatch) -> Vec<i64> {
    open_arrow_access_test_support::record::collect_values::<Int64Type>(
        std::slice::from_ref(batch),
        0,
    )
}

#[derive(Default)]
struct Observations {
    byte_options: Mutex<Vec<ByteReadOptions>>,
    batch_reads: AtomicUsize,
    max_batch_ranges: AtomicUsize,
    range_reads: AtomicUsize,
    fail_reads: AtomicBool,
    failure_kind: Mutex<Option<ErrorKind>>,
    drops: AtomicUsize,
}

/// A test-only layer validating range delegation and failure/cancellation behavior.
struct ByteObserverLayer(Arc<Observations>);

struct ByteObserverAccessor<A> {
    inner: A,
    observations: Arc<Observations>,
}

impl<A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>> Layer<A>
    for ByteObserverLayer
{
    type Layered = ByteObserverAccessor<A>;

    fn layer(&self, inner: A) -> Self::Layered {
        ByteObserverAccessor {
            inner,
            observations: self.0.clone(),
        }
    }
}

#[async_trait]
impl<A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>> Accessor
    for ByteObserverAccessor<A>
{
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        let inner = self.inner.capability();
        Capability {
            read: inner.read,
            read_with_range: inner.read_with_range,
            read_parallel: inner.read_parallel,
            list: inner.list,
        }
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        self.observations
            .byte_options
            .lock()
            .unwrap()
            .push(request.options);
        Ok(Box::new(ByteObserverReader {
            inner: self.inner.open(request).await?,
            observations: self.observations.clone(),
        }) as Box<dyn ByteReader>)
    }
}

struct ByteObserverReader {
    inner: Box<dyn ByteReader>,
    observations: Arc<Observations>,
}

#[async_trait]
impl<A: ByteAccessor> ByteAccessor for ByteObserverAccessor<A> {
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        self.inner.list(request).await
    }
}

impl Drop for ByteObserverReader {
    fn drop(&mut self) {
        self.observations.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl Reader for ByteObserverReader {
    type Item = Bytes;
    type Metadata = ByteMetadata;

    fn capability(&self) -> Capability {
        let inner = self.inner.capability();
        Capability {
            read: inner.read,
            read_with_range: inner.read_with_range,
            read_parallel: inner.read_parallel,
            list: inner.list,
        }
    }

    fn metadata(&self) -> &ByteMetadata {
        self.inner.metadata()
    }

    async fn next(&mut self) -> Result<Option<Bytes>> {
        self.inner.next().await
    }
}

#[async_trait]
impl ByteReader for ByteObserverReader {
    async fn read_ranges(&mut self, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        self.observations.batch_reads.fetch_add(1, Ordering::SeqCst);
        self.observations
            .max_batch_ranges
            .fetch_max(ranges.len(), Ordering::SeqCst);
        self.observations
            .range_reads
            .fetch_add(ranges.len(), Ordering::SeqCst);
        if self.observations.fail_reads.load(Ordering::SeqCst) {
            return Err(Error::new(
                self.observations
                    .failure_kind
                    .lock()
                    .unwrap()
                    .unwrap_or(ErrorKind::Io),
                "read_ranges",
                "injected read failure",
            )
            .with_source(std::io::Error::other("injected source failure")));
        }
        self.inner.read_ranges(ranges).await
    }

    async fn read_range(&mut self, range: Range<u64>) -> Result<Bytes> {
        self.observations.range_reads.fetch_add(1, Ordering::SeqCst);
        if self.observations.fail_reads.load(Ordering::SeqCst) {
            return Err(Error::new(
                self.observations
                    .failure_kind
                    .lock()
                    .unwrap()
                    .unwrap_or(ErrorKind::Io),
                "read_range",
                "injected read failure",
            )
            .with_source(std::io::Error::other("injected source failure")));
        }
        self.inner.read_range(range).await
    }
}

/// Test-only wrapper modeling a decoder that cannot preserve upstream byte ranges.
struct SequentialLayer;

struct SequentialAccessor<A>(A);

impl<A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>> Layer<A> for SequentialLayer {
    type Layered = SequentialAccessor<A>;

    fn layer(&self, inner: A) -> Self::Layered {
        SequentialAccessor(inner)
    }
}

#[async_trait]
impl<A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>> Accessor
    for SequentialAccessor<A>
{
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        Capability {
            read: self.0.capability().read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        Ok(Box::new(SequentialReader(self.0.open(request).await?)) as Box<dyn ByteReader>)
    }
}

struct SequentialReader(Box<dyn ByteReader>);

#[async_trait]
impl Reader for SequentialReader {
    type Item = Bytes;
    type Metadata = ByteMetadata;

    fn capability(&self) -> Capability {
        Capability {
            read: self.0.capability().read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    fn metadata(&self) -> &ByteMetadata {
        self.0.metadata()
    }

    async fn next(&mut self) -> Result<Option<Bytes>> {
        self.0.next().await
    }
}

#[async_trait]
impl ByteReader for SequentialReader {}

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

async fn rows(mut reader: Box<dyn RecordReader>) -> Vec<RecordBatch> {
    let mut rows = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        rows.extend((0..batch.num_rows()).map(|row| batch.slice(row, 1)));
    }
    rows
}

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
    let kind = ErrorKind::PermissionDenied;
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

#[tokio::test]
async fn open_and_planning_reject_unusable_inputs_with_their_own_error_context() {
    for bytes in [
        MemoryAccessor::new(b"", 1),
        MemoryAccessor::new(b"", 1)
            .with_ranges()
            .without_content_length(),
    ] {
        let observations = bytes.observations.clone();
        let accessor = ParquetAccessor::new(bytes);
        let request = ReadRequest::new("data.parquet");
        let open_error = accessor.open(request.clone()).await.err().unwrap();
        let plan_error = accessor.plan_scan(request).await.err().unwrap();
        for (error, operation, message) in [
            (
                open_error,
                "open_parquet",
                "Parquet requires range reads and a known byte length; materialize sequential input first",
            ),
            (
                plan_error,
                "plan_parquet",
                "Parquet partitions require range reads and a known byte length",
            ),
        ] {
            assert_eq!(error.kind(), ErrorKind::Unsupported);
            assert_eq!(error.operation(), operation);
            assert!(error.to_string().contains(message));
        }
        assert_eq!(observations.reads.load(Ordering::SeqCst), 0);
        assert_eq!(observations.range_reads.load(Ordering::SeqCst), 0);
    }
}

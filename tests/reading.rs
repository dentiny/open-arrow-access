mod support;

use std::{
    error::Error as _,
    num::NonZeroUsize,
    sync::{Arc, Mutex, atomic::Ordering},
};

use arrow_array::{Int64Array, StringArray};
use futures::TryStreamExt;
use more_asserts::{assert_gt, assert_le};
use open_arrow_access::{
    Accessor, AccessorExt, ByteAccessor, ByteReadOptions, ByteReadRequest, Capability, ErrorKind,
    Layer, ListRequest, ObjectRef, ObjectStream, ReadOptions, ReadRequest, Result, async_trait,
    file_format::ParquetAccessor, into_stream, storage::OpenDalAccessor,
};
use open_arrow_access_test_support::format;
use parquet::basic::Compression;
use support::{
    ByteObserverLayer, MemoryAccessor, Observations, SequentialLayer, fixture, fixture_path, ids,
};

#[tokio::test]
async fn local_parquet_preserves_values_nulls_and_order_across_row_groups() {
    for compression in [Compression::UNCOMPRESSED, Compression::SNAPPY] {
        let fixture = fixture(compression).await;
        let accessor = ParquetAccessor::new(OpenDalAccessor::new(
            open_arrow_access_test_support::fs_operator(fixture.dir.path()),
        ));
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
    let values = batches
        .iter()
        .flat_map(|b| {
            b.column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
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
async fn byte_reader_has_bounded_chunks_and_independent_range_cursor() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(dir.path().join("data.bin"), b"abcdefghij")
        .await
        .unwrap();
    let fs = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(dir.path()))
        .with_chunk_size(NonZeroUsize::new(3).unwrap());
    let mut reader = fs.open("data.bin".into()).await.unwrap();
    assert_eq!(reader.metadata().content_length, Some(10));
    assert!(reader.capability().read_with_range);
    assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), b"abc");
    assert_eq!(reader.read_range(7..10).await.unwrap().as_ref(), b"hij");
    assert!(reader.read_range(5..5).await.unwrap().is_empty());
    assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), b"def");
    let remaining = into_stream(reader).try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(
        remaining
            .iter()
            .flat_map(|b| b.iter().copied())
            .collect::<Vec<_>>(),
        b"ghij"
    );
}

#[tokio::test]
async fn filesystem_rejects_missing_files_directories_and_invalid_locators() {
    let dir = tempfile::tempdir().unwrap();
    let fs = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(dir.path()));
    let error = fs.open("missing.parquet".into()).await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::NotFound);
    assert!(error.source().is_some());
    for locator in ["", "../outside", "/absolute", "."] {
        let error = fs.open(locator.into()).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
    tokio::fs::create_dir(dir.path().join("subdir"))
        .await
        .unwrap();
    assert_eq!(
        fs.open("subdir/".into()).await.err().unwrap().kind(),
        ErrorKind::InvalidInput
    );
    assert!(fs.capability().list);
    assert!(
        fs.list("".into())
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn empty_file_and_invalid_ranges_have_explicit_behavior() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(dir.path().join("empty"), [])
        .await
        .unwrap();
    let fs = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(dir.path()));
    let mut reader = fs.open("empty".into()).await.unwrap();
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.read_range(0..0).await.unwrap().is_empty());
    for range in [0..1, std::ops::Range { start: 2, end: 1 }] {
        assert_eq!(
            reader.read_range(range).await.err().unwrap().kind(),
            ErrorKind::InvalidInput
        );
    }
    let mut bytes = fs.open("empty".into()).await.unwrap();
    assert!(bytes.next().await.unwrap().is_none());
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
async fn parquet_accepts_third_party_accessor_and_rejects_sequential_only_input() {
    let fixture = fixture(Compression::UNCOMPRESSED).await;
    let bytes = tokio::fs::read(fixture.dir.path().join("data.parquet"))
        .await
        .unwrap();
    let accessor = ParquetAccessor::new(MemoryAccessor::new(&bytes, usize::MAX).with_ranges());
    let reader = accessor
        .open(ReadRequest::new("opaque:fixture"))
        .await
        .unwrap();
    let batches = into_stream(reader).try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(
        batches.iter().flat_map(ids).collect::<Vec<_>>(),
        (0..7).collect::<Vec<_>>()
    );
    let accessor = ParquetAccessor::new(MemoryAccessor::new(b"sequential bytes", usize::MAX));
    assert_eq!(
        accessor
            .open(ReadRequest::new("anything"))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Unsupported
    );
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

struct ListingAccessor(MemoryAccessor);

#[async_trait]
impl Accessor for ListingAccessor {
    type Request = ByteReadRequest;
    type Reader = dyn open_arrow_access::ByteReader;

    fn capability(&self) -> Capability {
        let inner = self.0.capability();
        Capability {
            read: inner.read,
            read_with_range: inner.read_with_range,
            read_parallel: inner.read_parallel,
            list: true,
        }
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<Self::Reader>> {
        self.0.open(request).await
    }
}

#[async_trait]
impl ByteAccessor for ListingAccessor {
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        let mut objects = vec![Ok("entry".into())];
        if request.recursive {
            objects.push(Ok("sub/entry".into()));
        }
        Ok(Box::pin(futures::stream::iter(objects)) as ObjectStream)
    }
}

#[tokio::test]
async fn listing_capability_survives_transparent_layer_and_shared_accessor() {
    let accessor: Arc<dyn ByteAccessor> = Arc::new(
        ListingAccessor(MemoryAccessor::new(b"data", usize::MAX).with_ranges())
            .layer(ByteObserverLayer(Arc::new(Observations::default())))
            .layer(OpenObserverLayer {
                label: "list-observer",
                events: Arc::new(Mutex::new(Vec::new())),
            }),
    );
    assert!(accessor.capability().list);
    let objects = accessor
        .list(ListRequest::new("").with_recursive(false))
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].locator(), "entry");
    let reader = accessor
        .open(objects.into_iter().next().unwrap().into())
        .await
        .unwrap();
    assert!(reader.capability().read_with_range);
    assert!(!reader.capability().list);
}

#[tokio::test]
async fn byte_io_options_preserve_range_order_empty_ranges_and_sequential_cursor() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(dir.path().join("data.bin"), b"abcdefghij")
        .await
        .unwrap();
    let fs = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(dir.path()))
        .with_chunk_size(NonZeroUsize::new(3).unwrap());
    for concurrency in [1, 4] {
        let mut reader = fs
            .open(
                ByteReadRequest::new("data.bin").with_options(ByteReadOptions {
                    io_concurrency: NonZeroUsize::new(concurrency).unwrap(),
                    io_chunk_size: NonZeroUsize::new(2),
                }),
            )
            .await
            .unwrap();
        assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), b"abc");
        let buffers = reader
            .read_ranges(vec![7..10, 0..3, 5..5, 2..6, 7..10])
            .await
            .unwrap();
        let actual = buffers
            .iter()
            .map(|buffer| buffer.as_ref())
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            vec![
                &b"hij"[..],
                &b"abc"[..],
                &b""[..],
                &b"cdef"[..],
                &b"hij"[..]
            ]
        );
        assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), b"def");
        assert!(reader.read_ranges(vec![]).await.unwrap().is_empty());
        assert_eq!(
            reader
                .read_ranges(vec![0..2, 0..11])
                .await
                .err()
                .unwrap()
                .kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(reader.read_range(4..8).await.unwrap().as_ref(), b"efgh");
    }
}

#[tokio::test]
async fn third_party_byte_reader_uses_the_bulk_range_fallback() {
    let accessor = MemoryAccessor::new(b"abcdefghij", usize::MAX).with_ranges();
    let mut reader = accessor.open("memory".into()).await.unwrap();
    let actual = reader.read_ranges(vec![7..10, 0..3, 4..4]).await.unwrap();
    assert_eq!(
        actual.iter().map(|v| v.as_ref()).collect::<Vec<_>>(),
        vec![&b"hij"[..], &b"abc"[..], &b""[..]]
    );
    assert_eq!(
        reader.next().await.unwrap().unwrap().as_ref(),
        b"abcdefghij"
    );
}

#[tokio::test]
async fn parquet_forwards_per_open_io_options_and_uses_bulk_ranges_through_layers() {
    let fixture = fixture(Compression::UNCOMPRESSED).await;
    let observations = Arc::new(Observations::default());
    let bytes = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(
        fixture.dir.path(),
    ))
    .layer(ByteObserverLayer(observations.clone()));
    let records = ParquetAccessor::new(bytes);
    for concurrency in [1, 3] {
        let options = ByteReadOptions {
            io_concurrency: NonZeroUsize::new(concurrency).unwrap(),
            io_chunk_size: NonZeroUsize::new(64),
        };
        let reader = records
            .open(ReadRequest::new("data.parquet").with_byte_options(options))
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
    for kind in [
        ErrorKind::InvalidInput,
        ErrorKind::Unsupported,
        ErrorKind::NotFound,
        ErrorKind::PermissionDenied,
        ErrorKind::Io,
        ErrorKind::Decode,
    ] {
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

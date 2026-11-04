#![cfg(all(feature = "storage-fs", feature = "format-parquet"))]

mod support;

use std::{
    error::Error as _,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use arrow_array::{Int64Array, StringArray};
use futures::TryStreamExt;
use open_arrow_access::{
    Accessor, AccessorExt, BoxFuture, ByteAccessor, Capability, ErrorKind, Layer, ObjectRef,
    ObjectStream, ReadOptions, ReadRequest, Result, file_format::ParquetAccessor, into_stream,
    storage::FsAccessor,
};
use parquet::basic::Compression;
use support::{ByteObserverLayer, MemoryAccessor, Observations, SequentialLayer, fixture, ids};

#[tokio::test]
async fn local_parquet_preserves_values_nulls_and_order_across_row_groups() {
    for compression in [Compression::UNCOMPRESSED, Compression::SNAPPY] {
        let fixture = fixture(compression);
        let accessor = ParquetAccessor::new(FsAccessor::new(fixture.dir.path()).unwrap());
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
            assert!(batch.num_rows() <= 2);
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
    let fixture = fixture(Compression::UNCOMPRESSED);
    let accessor = ParquetAccessor::new(FsAccessor::new(fixture.dir.path()).unwrap());
    let reader = accessor
        .open(ReadRequest::new("data.parquet").with_options(ReadOptions {
            projection: Some(vec!["title".into(), "id".into()]),
            ..ReadOptions::default()
        }))
        .await
        .unwrap();
    assert_eq!(reader.metadata().schema.field(0).name(), "title");
    assert_eq!(reader.metadata().schema.field(1).name(), "id");
    let batches = into_stream(reader).try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 7);
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
    let fixture = fixture(Compression::UNCOMPRESSED);
    let accessor = ParquetAccessor::new(FsAccessor::new(fixture.dir.path()).unwrap());
    let reader = accessor
        .open(ReadRequest::new("data.parquet").with_options(ReadOptions {
            projection: Some(vec![]),
            ..ReadOptions::default()
        }))
        .await
        .unwrap();
    assert!(reader.metadata().schema.fields().is_empty());
    let batches = into_stream(reader).try_collect::<Vec<_>>().await.unwrap();
    assert!(batches.iter().all(|b| b.num_columns() == 0));
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 7);
}

#[tokio::test]
async fn projection_rejects_unknown_and_duplicate_fields() {
    let fixture = fixture(Compression::UNCOMPRESSED);
    let accessor = ParquetAccessor::new(FsAccessor::new(fixture.dir.path()).unwrap());
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
    std::fs::write(dir.path().join("data.bin"), b"abcdefghij").unwrap();
    let fs = FsAccessor::new(dir.path())
        .unwrap()
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
    let fs = FsAccessor::new(dir.path()).unwrap();
    let error = fs.open("missing.parquet".into()).await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::NotFound);
    assert!(error.source().is_some());
    for locator in ["", "../outside", "/absolute", "."] {
        let error = fs.open(locator.into()).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
    std::fs::create_dir(dir.path().join("subdir")).unwrap();
    assert_eq!(
        fs.open("subdir/".into()).await.err().unwrap().kind(),
        ErrorKind::InvalidInput
    );
    assert!(!fs.capability().list);
    assert_eq!(
        fs.list("".into()).await.err().unwrap().kind(),
        ErrorKind::Unsupported
    );
    assert!(FsAccessor::new("relative/root").is_err());
}

#[tokio::test]
async fn empty_file_and_invalid_ranges_have_explicit_behavior() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("empty"), []).unwrap();
    let fs = FsAccessor::new(dir.path()).unwrap();
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
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("broken.parquet"), b"not a parquet file").unwrap();
    let accessor = ParquetAccessor::new(FsAccessor::new(dir.path()).unwrap());
    let error = accessor
        .open(ReadRequest::new("broken.parquet"))
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::Decode);
    assert_eq!(error.operation(), "inspect_parquet");
    assert!(error.source().is_some());
}

#[tokio::test]
async fn parquet_accepts_third_party_accessor_and_rejects_sequential_only_input() {
    let fixture = fixture(Compression::UNCOMPRESSED);
    let bytes = std::fs::read(fixture.dir.path().join("data.parquet"))
        .unwrap()
        .into();
    let accessor = ParquetAccessor::new(MemoryAccessor::new(bytes, true));
    let reader = accessor
        .open(ReadRequest::new("opaque:fixture"))
        .await
        .unwrap();
    let batches = into_stream(reader).try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(
        batches.iter().flat_map(ids).collect::<Vec<_>>(),
        (0..7).collect::<Vec<_>>()
    );
    let accessor = ParquetAccessor::new(MemoryAccessor::new("sequential bytes".into(), false));
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
    let fixture = fixture(Compression::UNCOMPRESSED);
    let observations = Arc::new(Observations::default());
    let byte_accessor = FsAccessor::new(fixture.dir.path())
        .unwrap()
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
    assert_eq!(error.kind(), ErrorKind::Decode);
    assert!(reader.next().await.unwrap().is_none());
    drop(reader);
    assert_eq!(
        observations.drops.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}

// The same generic layer implementation applies to byte AND record accessors.
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

    /// Capture the event label while wrapping either a byte or a record accessor.
    fn layer(&self, inner: A) -> Self::Layered {
        OpenObserverAccessor {
            inner,
            label: self.label,
            events: self.events.clone(),
        }
    }
}

impl<A: Accessor> Accessor for OpenObserverAccessor<A> {
    type Request = A::Request;
    type Reader = A::Reader;

    /// Forward effective capabilities because observation preserves supported operations.
    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    /// Record wrapper call order and forward the request and returned reader unchanged.
    fn open(&self, request: Self::Request) -> BoxFuture<'_, Result<Box<Self::Reader>>> {
        self.events.lock().unwrap().push(self.label);
        self.inner.open(request)
    }
}

impl<A: ByteAccessor> ByteAccessor for OpenObserverAccessor<A> {
    /// Delegate discovery so wrapping does not introduce or hide unsupported behavior.
    fn list(&self, prefix: ObjectRef) -> BoxFuture<'_, Result<ObjectStream>> {
        self.inner.list(prefix)
    }
}

#[tokio::test]
async fn one_layer_contract_composes_at_byte_and_record_boundaries_in_known_order() {
    let fixture = fixture(Compression::UNCOMPRESSED);
    let events = Arc::new(Mutex::new(Vec::new()));
    let layer = |label| OpenObserverLayer {
        label,
        events: events.clone(),
    };
    let bytes = FsAccessor::new(fixture.dir.path())
        .unwrap()
        .layer(layer("inner-byte"))
        .layer(layer("outer-byte"));
    assert!(!bytes.capability().list);
    assert_eq!(
        bytes.list("".into()).await.err().unwrap().kind(),
        ErrorKind::Unsupported
    );
    // Also verify object-safe injection through an Arc, as third-party adapters use.
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
    let fixture = fixture(Compression::UNCOMPRESSED);
    let fs = FsAccessor::new(fixture.dir.path()).unwrap();
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

impl Accessor for ListingAccessor {
    type Request = ObjectRef;
    type Reader = dyn open_arrow_access::ByteReader;

    /// Add implemented object discovery to the underlying fixture's read abilities.
    fn capability(&self) -> Capability {
        Capability {
            list: true,
            ..self.0.capability()
        }
    }

    /// Delegate opening; reader capabilities intentionally do not include listing.
    fn open(&self, request: ObjectRef) -> BoxFuture<'_, Result<Box<Self::Reader>>> {
        self.0.open(request)
    }
}

impl ByteAccessor for ListingAccessor {
    /// Return one discovered object matching the fixture's sole namespace.
    fn list(&self, _prefix: ObjectRef) -> BoxFuture<'_, Result<ObjectStream>> {
        Box::pin(async {
            Ok(Box::pin(futures::stream::iter([Ok("entry".into())])) as ObjectStream)
        })
    }
}

#[tokio::test]
async fn listing_capability_survives_transparent_layer_and_shared_accessor() {
    let accessor: Arc<dyn ByteAccessor> = Arc::new(
        ListingAccessor(MemoryAccessor::new("data".into(), true))
            .layer(ByteObserverLayer(Arc::new(Observations::default())))
            .layer(OpenObserverLayer {
                label: "list-observer",
                events: Arc::new(Mutex::new(Vec::new())),
            }),
    );
    assert!(accessor.capability().list);
    let objects = accessor
        .list("".into())
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].locator(), "entry");
    let reader = accessor
        .open(objects.into_iter().next().unwrap())
        .await
        .unwrap();
    assert!(reader.capability().read_with_range);
    assert!(!reader.capability().list);
}

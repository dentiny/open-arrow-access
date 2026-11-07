use std::{
    error::Error as _,
    num::NonZeroUsize,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow_array::{Array, Int64Array, StringArray};
use arrow_schema::{ArrowError, DataType, Field, IntervalUnit, Schema, TimeUnit};
use more_asserts::assert_lt;
use open_arrow_access_core::{
    Accessor, ByteMetadata, ByteReadOptions, ByteReadRequest, ByteReader, Bytes, Capability, Error,
    ErrorKind, ReadOptions, ReadRequest, Reader, RecordBatch, Result, SchemaRef, async_trait,
};

use open_arrow_access_file_format::JsonAccessor;
use open_arrow_access_test_support::{MemoryAccessor, Observations};

fn schema() -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("title", DataType::Utf8, true),
        ],
        [("fixture".to_owned(), "json".to_owned())].into(),
    ))
}

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../testdata/json")
        .join(name)
}

async fn fixture_data(name: &str) -> Vec<u8> {
    let path = fixture_path(name);
    tokio::fs::read(&path)
        .await
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

async fn fixture(name: &str, chunk_size: usize) -> (MemoryAccessor, Arc<Observations>) {
    let input = MemoryAccessor::new(&fixture_data(name).await, chunk_size);
    let observations = input.observations.clone();
    (input, observations)
}

fn request(batch_size: usize, projection: Option<Vec<&str>>) -> ReadRequest {
    ReadRequest::new("records.json").with_options(ReadOptions {
        batch_size: NonZeroUsize::new(batch_size).unwrap(),
        projection: projection.map(|names| names.into_iter().map(str::to_owned).collect()),
    })
}

fn ids(batch: &RecordBatch) -> &[i64] {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
}

#[tokio::test]
async fn fragmented_json_objects_preserve_values_schema_and_batch_order() {
    for chunk_size in [1, 7, 4096] {
        let (input, _) = fixture("good.json", chunk_size).await;
        let accessor = JsonAccessor::new(input, schema());
        let mut reader = accessor.open(request(2, None)).await.unwrap();
        assert_eq!(reader.metadata().schema, schema());
        assert_eq!(
            reader.metadata().source.as_ref().unwrap().locator(),
            "records.json"
        );
        let first = reader.next().await.unwrap().unwrap();
        assert_eq!(first.schema(), schema());
        assert_eq!(ids(&first), [0, 1]);
        let titles = first
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(titles.value(0), "雪\nline");
        assert!(titles.is_null(1));
        let last = reader.next().await.unwrap().unwrap();
        assert_eq!(ids(&last), [2, 3]);
        assert!(last.column(1).is_null(0));
        assert!(reader.next().await.unwrap().is_none());
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn projection_preserves_requested_order_metadata_and_zero_column_row_counts() {
    for projection in [vec!["title", "id"], vec![]] {
        let (input, _) = fixture("projection.jsonl", 3).await;
        let accessor = JsonAccessor::new(input, schema());
        let mut reader = accessor
            .open(request(1, Some(projection.clone())))
            .await
            .unwrap();
        for id in [1, 2] {
            let batch = reader.next().await.unwrap().unwrap();
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), projection.len());
            assert_eq!(batch.schema(), reader.metadata().schema);
            assert_eq!(batch.schema().metadata(), schema().metadata());
            if !projection.is_empty() {
                assert_eq!(batch.schema().field(0).name(), "title");
                assert_eq!(
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                    id
                );
            }
        }
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn invalid_projection_fails_before_opening_bytes() {
    let (input, observed) = fixture("empty.json", 1).await;
    let accessor = JsonAccessor::new(input, schema());
    for projection in [vec!["missing"], vec!["id", "id"]] {
        let error = accessor
            .open(request(2, Some(projection)))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.operation(), "project");
    }
    assert!(observed.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn malformed_truncated_and_non_object_inputs_fail_terminally() {
    for name in [
        "bad-empty-array.json",
        "bad-object-array.json",
        "bad-null.json",
        "bad-number.json",
        "bad-boolean.json",
        "bad-string.json",
        "bad-truncated-value.json",
        "bad-truncated-object.json",
        "bad-trailing.json",
        "bad-type.json",
        "bad-utf8.json",
    ] {
        for projection in [None, Some(vec![])] {
            // A projected-away field's value is deliberately not decoded.
            if matches!(name, "bad-type.json" | "bad-utf8.json") && projection.is_some() {
                continue;
            }
            let (input, observed) = fixture(name, 1).await;
            let accessor = JsonAccessor::new(input, schema());
            let mut reader = accessor.open(request(10, projection)).await.unwrap();
            let error = reader
                .next()
                .await
                .err()
                .unwrap_or_else(|| panic!("accepted {name}"));
            assert_eq!(error.kind(), ErrorKind::Decode, "{name}");
            assert_eq!(error.operation(), "decode_json", "{name}");
            assert!(error.source().unwrap().is::<ArrowError>(), "{name}");
            let reads = observed.reads.load(Ordering::SeqCst);
            assert!(reader.next().await.unwrap().is_none());
            assert_eq!(observed.reads.load(Ordering::SeqCst), reads);
        }
    }
}

#[tokio::test]
async fn io_errors_keep_their_category_after_a_successful_batch() {
    let (input, observed) = fixture("io-failure.jsonl", 9).await;
    let input = input.with_failure(9, ErrorKind::Io);
    let accessor = JsonAccessor::new(input, schema());
    let mut reader = accessor.open(request(1, None)).await.unwrap();
    assert_eq!(ids(&reader.next().await.unwrap().unwrap()), [1]);
    let error = reader.next().await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::Io);
    assert_eq!(error.operation(), "memory_read");
    let reads = observed.reads.load(Ordering::SeqCst);
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(observed.reads.load(Ordering::SeqCst), reads);
}

#[tokio::test]
async fn per_open_options_are_forwarded_and_sequential_bytes_are_sufficient() {
    let (input, observed) = fixture("single.json", 1).await;
    let accessor = JsonAccessor::new(input, schema());
    assert!(accessor.capability().read);
    assert!(!accessor.capability().read_with_range);
    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        io_chunk_size: NonZeroUsize::new(17),
    };
    let mut reader = accessor
        .open(request(1, None).with_byte_options(options))
        .await
        .unwrap();
    assert_eq!(observed.requests.lock().unwrap()[0].options, options);
    assert!(!reader.capability().read_with_range);
    assert_eq!(ids(&reader.next().await.unwrap().unwrap()), [1]);
}

#[tokio::test]
async fn reading_a_batch_does_not_collect_the_entire_input() {
    let data = fixture_data("many.jsonl").await;
    let input = MemoryAccessor::new(&data, 1);
    let observed = input.observations.clone();
    let accessor = JsonAccessor::new(input, schema());
    let mut reader = accessor.open(request(2, None)).await.unwrap();
    assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
    assert_eq!(ids(&reader.next().await.unwrap().unwrap()), [0, 1]);
    assert_lt!(observed.reads.load(Ordering::SeqCst), data.len());
    let reads = observed.reads.load(Ordering::SeqCst);
    drop(reader);
    assert_eq!(observed.reads.load(Ordering::SeqCst), reads);
}

#[tokio::test]
async fn empty_and_whitespace_only_inputs_have_no_rows() {
    for name in ["empty.json", "whitespace.json"] {
        let (input, observed) = fixture(name, 1).await;
        let accessor = JsonAccessor::new(input, schema());
        let mut reader = accessor.open(request(1, None)).await.unwrap();
        assert!(reader.next().await.unwrap().is_none());
        let reads = observed.reads.load(Ordering::SeqCst);
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(observed.reads.load(Ordering::SeqCst), reads);
    }
}

#[tokio::test]
async fn local_jsonl_reads_through_opendal() {
    let storage = open_arrow_access_storage::OpenDalAccessor::new(
        open_arrow_access_test_support::fs_operator(fixture_path("")).await,
    );
    let accessor = JsonAccessor::new(
        storage.with_chunk_size(NonZeroUsize::new(2).unwrap()),
        schema(),
    );
    let mut reader = accessor.open(ReadRequest::new("good.jsonl")).await.unwrap();
    assert_eq!(ids(&reader.next().await.unwrap().unwrap()), [1, 2]);
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn decoder_configuration_errors_are_classified_before_opening_bytes() {
    for (data_type, expected) in [
        (
            DataType::Interval(IntervalUnit::DayTime),
            ErrorKind::Unsupported,
        ),
        (
            DataType::Timestamp(TimeUnit::Millisecond, Some("invalid/timezone".into())),
            ErrorKind::InvalidInput,
        ),
    ] {
        let (input, observed) = fixture("empty.json", 1).await;
        let schema = Arc::new(Schema::new(vec![Field::new("value", data_type, true)]));
        let accessor = JsonAccessor::new(input, schema);
        let error = accessor.open(request(1, None)).await.err().unwrap();
        assert_eq!(error.kind(), expected);
        assert_eq!(error.operation(), "open_json");
        assert!(error.source().unwrap().is::<ArrowError>());
        assert!(observed.requests.lock().unwrap().is_empty());
        assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
    }
}

#[derive(Clone, Copy)]
enum Fault {
    Open(ErrorKind),
    Read { after: usize, kind: ErrorKind },
    EmptyChunk,
}

struct FaultyAccessor {
    inner: MemoryAccessor,
    fault: Fault,
    polls: Arc<AtomicUsize>,
}

fn source_error(kind: ErrorKind) -> Error {
    Error::new(kind, "fixture_source", "injected source failure").with_source(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "connection closed",
    ))
}

fn assert_source_error(error: &Error, kind: ErrorKind) {
    assert_eq!(error.kind(), kind);
    assert_eq!(error.operation(), "fixture_source");
    assert_eq!(
        error
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .kind(),
        std::io::ErrorKind::ConnectionReset,
    );
}

#[async_trait]
impl Accessor for FaultyAccessor {
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
        if let Fault::Open(kind) = self.fault {
            return Err(source_error(kind));
        }
        Ok(Box::new(FaultyReader {
            inner: self.inner.open(request).await?,
            fault: self.fault,
            polls: self.polls.clone(),
            consumed: 0,
        }))
    }
}

struct FaultyReader {
    inner: Box<dyn ByteReader>,
    fault: Fault,
    polls: Arc<AtomicUsize>,
    consumed: usize,
}

#[async_trait]
impl Reader for FaultyReader {
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
        self.polls.fetch_add(1, Ordering::SeqCst);
        match self.fault {
            Fault::Read { after, kind } if self.consumed >= after => {
                return Err(source_error(kind));
            }
            Fault::EmptyChunk => return Ok(Some(Bytes::new())),
            _ => {}
        }
        let bytes = self.inner.next().await?;
        if let Some(bytes) = &bytes {
            self.consumed += bytes.len();
        }
        Ok(bytes)
    }
}

#[async_trait]
impl ByteReader for FaultyReader {}

#[tokio::test]
async fn source_errors_preserve_category_and_cause_at_open_and_mid_record() {
    for kind in [
        ErrorKind::NotFound,
        ErrorKind::PermissionDenied,
        ErrorKind::Io,
        ErrorKind::Unsupported,
        ErrorKind::InvalidInput,
        ErrorKind::Decode,
    ] {
        for fault in [Fault::Open(kind), Fault::Read { after: 5, kind }] {
            let (input, _) = fixture("good.json", 1).await;
            let polls = Arc::new(AtomicUsize::new(0));
            let accessor = JsonAccessor::new(
                FaultyAccessor {
                    inner: input,
                    fault,
                    polls: polls.clone(),
                },
                schema(),
            );
            if let Fault::Open(_) = fault {
                let error = accessor.open(request(1, None)).await.err().unwrap();
                assert_source_error(&error, kind);
                assert_eq!(polls.load(Ordering::SeqCst), 0);
            } else {
                let mut reader = accessor.open(request(1, None)).await.unwrap();
                let error = reader.next().await.err().unwrap();
                assert_source_error(&error, kind);
                let calls = polls.load(Ordering::SeqCst);
                assert!(reader.next().await.unwrap().is_none());
                assert!(reader.next().await.unwrap().is_none());
                assert_eq!(polls.load(Ordering::SeqCst), calls);
            }
        }
    }
}

#[tokio::test]
async fn empty_source_chunks_are_io_errors_and_terminal() {
    let (input, _) = fixture("single.json", 1).await;
    let polls = Arc::new(AtomicUsize::new(0));
    let accessor = JsonAccessor::new(
        FaultyAccessor {
            inner: input,
            fault: Fault::EmptyChunk,
            polls: polls.clone(),
        },
        schema(),
    );
    let mut reader = accessor.open(request(1, None)).await.unwrap();
    let error = reader.next().await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::Io);
    assert_eq!(error.operation(), "read_json");
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(polls.load(Ordering::SeqCst), 1);
}

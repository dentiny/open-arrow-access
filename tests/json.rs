#![cfg(feature = "format-json")]

use std::{
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow_array::{Array, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use open_arrow_access::{
    Accessor, ByteMetadata, ByteReadOptions, ByteReadRequest, ByteReader, Bytes, Capability, Error,
    ErrorKind, ReadOptions, ReadRequest, Reader, RecordBatch, Result, SchemaRef, async_trait,
    file_format::JsonAccessor,
};

#[derive(Default)]
struct Observations {
    requests: Mutex<Vec<ByteReadRequest>>,
    reads: AtomicUsize,
}

struct MemoryAccessor {
    data: Bytes,
    chunk_size: usize,
    fail_after: Option<usize>,
    observations: Arc<Observations>,
}

#[async_trait]
impl Accessor for MemoryAccessor {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            ..Capability::default()
        }
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        self.observations
            .requests
            .lock()
            .unwrap()
            .push(request.clone());
        Ok(Box::new(MemoryReader {
            data: self.data.clone(),
            chunk_size: self.chunk_size,
            offset: 0,
            fail_after: self.fail_after,
            observations: self.observations.clone(),
            metadata: ByteMetadata {
                source: Some(request.object),
                ..ByteMetadata::default()
            },
        }))
    }
}

struct MemoryReader {
    data: Bytes,
    chunk_size: usize,
    offset: usize,
    fail_after: Option<usize>,
    observations: Arc<Observations>,
    metadata: ByteMetadata,
}

#[async_trait]
impl Reader for MemoryReader {
    type Item = Bytes;
    type Metadata = ByteMetadata;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            ..Capability::default()
        }
    }

    fn metadata(&self) -> &ByteMetadata {
        &self.metadata
    }

    async fn next(&mut self) -> Result<Option<Bytes>> {
        self.observations.reads.fetch_add(1, Ordering::SeqCst);
        if self.fail_after.is_some_and(|offset| self.offset >= offset) {
            return Err(Error::new(ErrorKind::Io, "memory_read", "injected failure"));
        }
        if self.offset == self.data.len() {
            return Ok(None);
        }
        let end = (self.offset + self.chunk_size).min(self.data.len());
        let bytes = self.data.slice(self.offset..end);
        self.offset = end;
        Ok(Some(bytes))
    }
}

#[async_trait]
impl ByteReader for MemoryReader {}

fn schema() -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("title", DataType::Utf8, true),
        ],
        [("fixture".to_owned(), "json".to_owned())].into(),
    ))
}

fn fixture(data: &str, chunk_size: usize) -> (MemoryAccessor, Arc<Observations>) {
    let observations = Arc::new(Observations::default());
    (
        MemoryAccessor {
            data: Bytes::copy_from_slice(data.as_bytes()),
            chunk_size,
            fail_after: None,
            observations: observations.clone(),
        },
        observations,
    )
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
    let data = "{\"id\":0,\"title\":\"雪\\nline\",\"ignored\":{\"x\":[1,2]}}\r\n\n{\"id\":1,\"title\":null} {\n\"id\":2\n}\n{\"id\":3,\"title\":\"last\"}";
    for chunk_size in [1, 7, 4096] {
        let (input, _) = fixture(data, chunk_size);
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
        let (input, _) = fixture("{\"id\":1,\"title\":\"one\"}\n{\"id\":2}", 3);
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
    let (input, observed) = fixture("", 1);
    let accessor = JsonAccessor::new(input, schema());
    for projection in [vec!["missing"], vec!["id", "id"]] {
        let error = accessor
            .open(request(2, Some(projection)))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
    assert!(observed.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn malformed_truncated_and_non_object_inputs_fail_terminally() {
    for data in [
        "[]",
        "[{}]",
        "null",
        "1",
        "true",
        "\"text\"",
        "{\"id\":",
        "{\"id\":1",
        "{\"id\":1} trailing",
        "{\"id\":\"bad\"}",
    ] {
        for projection in [None, Some(vec![])] {
            // A projected-away field's value type is deliberately not checked.
            if data == "{\"id\":\"bad\"}" && projection.is_some() {
                continue;
            }
            let (input, observed) = fixture(data, 1);
            let accessor = JsonAccessor::new(input, schema());
            let mut reader = accessor.open(request(10, projection)).await.unwrap();
            let error = reader
                .next()
                .await
                .err()
                .unwrap_or_else(|| panic!("accepted {data:?}"));
            assert_eq!(error.kind(), ErrorKind::Decode, "{data:?}");
            let reads = observed.reads.load(Ordering::SeqCst);
            assert!(reader.next().await.unwrap().is_none());
            assert_eq!(observed.reads.load(Ordering::SeqCst), reads);
        }
    }
}

#[tokio::test]
async fn io_errors_keep_their_category_after_a_successful_batch() {
    let data = "{\"id\":1}\n{\"id\":2}";
    let (mut input, observed) = fixture(data, 9);
    input.fail_after = Some(9);
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
    let (input, observed) = fixture("{\"id\":1}", 1);
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
    let data = (0..100)
        .map(|id| format!("{{\"id\":{id}}}\n"))
        .collect::<String>();
    let (input, observed) = fixture(&data, 1);
    let accessor = JsonAccessor::new(input, schema());
    let mut reader = accessor.open(request(2, None)).await.unwrap();
    assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
    assert_eq!(ids(&reader.next().await.unwrap().unwrap()), [0, 1]);
    assert!(observed.reads.load(Ordering::SeqCst) < data.len());
    let reads = observed.reads.load(Ordering::SeqCst);
    drop(reader);
    assert_eq!(observed.reads.load(Ordering::SeqCst), reads);
}

#[tokio::test]
async fn empty_and_whitespace_only_inputs_have_no_rows() {
    for data in ["", "\r\n \t"] {
        let (input, observed) = fixture(data, 1);
        let accessor = JsonAccessor::new(input, schema());
        let mut reader = accessor.open(request(1, None)).await.unwrap();
        assert!(reader.next().await.unwrap().is_none());
        let reads = observed.reads.load(Ordering::SeqCst);
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(observed.reads.load(Ordering::SeqCst), reads);
    }
}

#[cfg(feature = "storage-fs")]
#[tokio::test]
async fn local_jsonl_reads_through_opendal() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("data.jsonl"), b"{\"id\":1}\n{\"id\":2}\n").unwrap();
    let storage = open_arrow_access::storage::FsAccessor::new(dir.path())
        .await
        .unwrap();
    let accessor = JsonAccessor::new(
        storage.with_chunk_size(NonZeroUsize::new(2).unwrap()),
        schema(),
    );
    let mut reader = accessor.open(ReadRequest::new("data.jsonl")).await.unwrap();
    assert_eq!(ids(&reader.next().await.unwrap().unwrap()), [1, 2]);
    assert!(reader.next().await.unwrap().is_none());
}

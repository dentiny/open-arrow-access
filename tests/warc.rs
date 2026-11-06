use std::{
    num::NonZeroUsize,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow_array::{Array, BinaryViewArray, StringArray, UInt64Array};
use open_arrow_access::{
    Accessor, ByteMetadata, ByteReadOptions, ByteReadRequest, ByteReader, Bytes, Capability, Error,
    ErrorKind, ReadOptions, ReadRequest, Reader, Result, async_trait,
    file_format::{WarcAccessor, warc_schema},
};

#[derive(Clone)]
struct Memory {
    bytes: Bytes,
    chunk_size: usize,
    polls: Arc<AtomicUsize>,
    options: Arc<Mutex<Vec<ByteReadOptions>>>,
    fail_at: Option<usize>,
}

impl Memory {
    fn new(bytes: Vec<u8>, chunk_size: usize) -> Self {
        Self {
            bytes: bytes.into(),
            chunk_size,
            polls: Arc::default(),
            options: Arc::default(),
            fail_at: None,
        }
    }
}

#[async_trait]
impl Accessor for Memory {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            ..Capability::default()
        }
    }
    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        self.options.lock().unwrap().push(request.options);
        Ok(Box::new(Input {
            inner: self.clone(),
            offset: 0,
            metadata: ByteMetadata {
                source: Some(request.object),
                content_length: Some(self.bytes.len() as u64),
                version: None,
            },
        }))
    }
}

struct Input {
    inner: Memory,
    offset: usize,
    metadata: ByteMetadata,
}

#[async_trait]
impl Reader for Input {
    type Item = Bytes;
    type Metadata = ByteMetadata;
    fn capability(&self) -> Capability {
        self.inner.capability()
    }
    fn metadata(&self) -> &ByteMetadata {
        &self.metadata
    }
    async fn next(&mut self) -> Result<Option<Bytes>> {
        self.inner.polls.fetch_add(1, Ordering::SeqCst);
        if self
            .inner
            .fail_at
            .is_some_and(|offset| self.offset >= offset)
        {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "fixture",
                "upstream denied",
            ));
        }
        if self.offset == self.inner.bytes.len() {
            return Ok(None);
        }
        let end = self
            .offset
            .saturating_add(self.inner.chunk_size)
            .min(self.inner.bytes.len());
        let bytes = self.inner.bytes.slice(self.offset..end);
        self.offset = end;
        Ok(Some(bytes))
    }
}
#[async_trait]
impl ByteReader for Input {}

fn fixture(name: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata/warc")
        .join(name);
    std::fs::read(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

fn rows(batch: &open_arrow_access::RecordBatch) -> Vec<&str> {
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    ids.iter().map(Option::unwrap).collect()
}

#[tokio::test]
async fn physical_record_types_preserve_binary_bodies_schema_nulls_and_order() {
    let input = fixture("good_physical_records.warc");
    let accessor = WarcAccessor::new(Memory::new(input, 1));
    let mut reader = accessor
        .open(ReadRequest::new("good_physical_records.warc"))
        .await
        .unwrap();
    assert_eq!(reader.metadata().schema, warc_schema());
    assert_eq!(reader.metadata().schema.fields().len(), 20);
    assert_eq!(
        reader.metadata().source.as_ref().unwrap().locator(),
        "good_physical_records.warc"
    );
    assert!(!reader.capability().read_with_range);
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(
        rows(&batch),
        ["<urn:test:first>", "<urn:test:second>", "<urn:test:third>"]
    );
    let body = batch
        .column(19)
        .as_any()
        .downcast_ref::<BinaryViewArray>()
        .unwrap();
    assert_eq!(body.value(0), b"");
    assert_eq!(body.value(1), &[0, 255, 13, 10]);
    assert_eq!(body.value(2), b"end");
    let content_type = batch
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(content_type.is_null(0));
    assert_eq!(content_type.value(1), "application/octet-stream");
    let lengths = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(lengths.values(), &[0, 4, 3]);
    let numbers = batch
        .column(16)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert!(numbers.is_null(0));
    assert_eq!(numbers.value(2), 2);
    assert_eq!(batch.schema(), reader.metadata().schema);
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn projection_preserves_requested_order_and_zero_column_row_count() {
    let input = fixture("good_request_response.warc");
    let accessor = WarcAccessor::new(Memory::new(input, 3));
    for columns in [vec!["body".to_owned(), "id".to_owned()], vec![]] {
        let mut reader = accessor
            .open(ReadRequest::new("a.warc").with_options(ReadOptions {
                projection: Some(columns.clone()),
                batch_size: NonZeroUsize::new(1).unwrap(),
            }))
            .await
            .unwrap();
        for _ in 0..2 {
            let batch = reader.next().await.unwrap().unwrap();
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), columns.len());
            assert_eq!(batch.schema(), reader.metadata().schema);
            for (field, expected) in batch.schema().fields().iter().zip(&columns) {
                assert_eq!(field.name(), expected);
            }
        }
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn invalid_projection_is_rejected_before_io() {
    let input = Memory::new(fixture("empty.warc"), 1);
    let options = input.options.clone();
    let accessor = WarcAccessor::new(input);
    for names in [vec!["id", "id"], vec!["unknown"]] {
        let result = accessor
            .open(ReadRequest::new("a.warc").with_options(ReadOptions {
                projection: Some(names.into_iter().map(str::to_owned).collect()),
                ..ReadOptions::default()
            }))
            .await;
        assert_eq!(result.err().unwrap().kind(), ErrorKind::InvalidInput);
    }
    assert!(options.lock().unwrap().is_empty());
}

#[tokio::test]
async fn byte_budget_cuts_batches_without_splitting_records() {
    let input = fixture("good_batch_budget.warc");
    let accessor = WarcAccessor::new(Memory::new(input, 7))
        .with_batch_body_bytes(NonZeroUsize::new(3).unwrap());
    let mut reader = accessor.open(ReadRequest::new("a.warc")).await.unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 2);
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 2);
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn malformed_truncated_and_invalid_numeric_inputs_fail_terminally() {
    for name in [
        "bad_incomplete_header.warc",
        "bad_truncated_body.warc",
        "bad_terminator.warc",
        "bad_missing_date.warc",
        "bad_segment_number.warc",
    ] {
        let accessor = WarcAccessor::new(Memory::new(fixture(name), 1));
        let mut reader = accessor.open(ReadRequest::new(name)).await.unwrap();
        assert_eq!(
            reader.next().await.err().unwrap().kind(),
            ErrorKind::Decode,
            "{name}"
        );
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn upstream_errors_retain_kind_after_a_delivered_batch() {
    let first = fixture("good_resource.warc");
    let bytes = fixture("good_resources.warc");
    let mut input = Memory::new(bytes, 1);
    input.fail_at = Some(first.len());
    let accessor = WarcAccessor::new(input);
    let mut reader = accessor
        .open(ReadRequest::new("a.warc").with_options(ReadOptions {
            batch_size: NonZeroUsize::new(1).unwrap(),
            ..ReadOptions::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        rows(&reader.next().await.unwrap().unwrap()),
        ["<urn:test:a>"]
    );
    let error = reader.next().await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn reading_is_lazy_forwards_byte_options_and_stops_on_drop() {
    let first = fixture("good_resource.warc");
    let input = Memory::new(fixture("good_resources.warc"), 1);
    let polls = input.polls.clone();
    let observed = input.options.clone();
    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        io_chunk_size: NonZeroUsize::new(5),
    };
    let accessor = WarcAccessor::new(input);
    let mut reader = accessor
        .open(
            ReadRequest::new("a.warc")
                .with_byte_options(options)
                .with_options(ReadOptions {
                    batch_size: NonZeroUsize::new(1).unwrap(),
                    ..ReadOptions::default()
                }),
        )
        .await
        .unwrap();
    assert_eq!(*observed.lock().unwrap(), [options]);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    reader.next().await.unwrap().unwrap();
    assert_eq!(polls.load(Ordering::SeqCst), first.len());
    drop(reader);
    assert_eq!(polls.load(Ordering::SeqCst), first.len());
}

#[tokio::test]
async fn empty_input_and_independent_opens_have_explicit_behavior() {
    let empty = WarcAccessor::new(Memory::new(fixture("empty.warc"), 1));
    let mut reader = empty.open(ReadRequest::new("empty.warc")).await.unwrap();
    assert!(reader.next().await.unwrap().is_none());
    let accessor = WarcAccessor::new(Memory::new(fixture("good_metadata.warc"), 2));
    let mut left = accessor.open(ReadRequest::new("a.warc")).await.unwrap();
    let mut right = accessor.open(ReadRequest::new("a.warc")).await.unwrap();
    assert_eq!(
        rows(&left.next().await.unwrap().unwrap()),
        rows(&right.next().await.unwrap().unwrap())
    );
}

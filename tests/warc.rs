use std::{
    error::Error as _,
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
    failure_kind: ErrorKind,
    fail_open: bool,
    empty_at: Option<usize>,
}

impl Memory {
    fn new(bytes: Vec<u8>, chunk_size: usize) -> Self {
        Self {
            bytes: bytes.into(),
            chunk_size,
            polls: Arc::default(),
            options: Arc::default(),
            fail_at: None,
            failure_kind: ErrorKind::PermissionDenied,
            fail_open: false,
            empty_at: None,
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
            read_with_range: false,
            list: false,
        }
    }
    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        self.options.lock().unwrap().push(request.options);
        if self.fail_open {
            return Err(source_error(self.failure_kind));
        }
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
            return Err(source_error(self.inner.failure_kind));
        }
        if self
            .inner
            .empty_at
            .is_some_and(|offset| self.offset >= offset)
        {
            return Ok(Some(Bytes::new()));
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
    let mut input = Memory::new(fixture("empty.warc"), 1);
    input.fail_open = true;
    let options = input.options.clone();
    let polls = input.polls.clone();
    let accessor = WarcAccessor::new(input);
    for names in [vec!["id", "id"], vec!["unknown"]] {
        let result = accessor
            .open(ReadRequest::new("a.warc").with_options(ReadOptions {
                projection: Some(names.into_iter().map(str::to_owned).collect()),
                ..ReadOptions::default()
            }))
            .await;
        let error = result.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.operation(), "project");
    }
    assert!(options.lock().unwrap().is_empty());
    assert_eq!(polls.load(Ordering::SeqCst), 0);
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
        let input = Memory::new(fixture(name), 1);
        let polls = input.polls.clone();
        let accessor = WarcAccessor::new(input);
        let mut reader = accessor.open(ReadRequest::new(name)).await.unwrap();
        let error = reader.next().await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::Decode, "{name}");
        assert_eq!(error.operation(), "decode_warc", "{name}");
        assert!(error.source().is_some(), "{name}");
        let calls = polls.load(Ordering::SeqCst);
        assert!(reader.next().await.unwrap().is_none());
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(polls.load(Ordering::SeqCst), calls, "{name}");
    }
}

#[tokio::test]
async fn upstream_errors_retain_kind_after_a_delivered_batch() {
    let first = fixture("good_resource.warc");
    let bytes = fixture("good_resources.warc");
    let mut input = Memory::new(bytes, 1);
    input.fail_at = Some(first.len());
    let polls = input.polls.clone();
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
    assert_wrapped_source_error(&error, ErrorKind::PermissionDenied);
    let calls = polls.load(Ordering::SeqCst);
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(polls.load(Ordering::SeqCst), calls);
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

fn source_error(kind: ErrorKind) -> Error {
    Error::new(kind, "fixture_source", "injected source failure").with_source(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "connection closed",
    ))
}

fn assert_original_source_error(error: &Error, kind: ErrorKind) {
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

fn assert_wrapped_source_error(error: &Error, kind: ErrorKind) {
    assert_eq!(error.kind(), kind);
    assert_eq!(error.operation(), "decode_warc");
    let parser_error = error.source().unwrap();
    assert_eq!(ErrorKind::from_source(parser_error), Some(kind));
    let io_error = parser_error
        .source()
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
    let original = io_error.get_ref().unwrap().downcast_ref::<Error>().unwrap();
    assert_original_source_error(original, kind);
}

#[tokio::test]
async fn source_errors_retain_category_and_cause_at_open_and_parser_boundaries() {
    let bytes = fixture("good_resource.warc");
    let body_start = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .unwrap()
        + 4;
    for kind in [
        ErrorKind::NotFound,
        ErrorKind::PermissionDenied,
        ErrorKind::Io,
        ErrorKind::Unsupported,
        ErrorKind::InvalidInput,
        ErrorKind::Decode,
    ] {
        let mut input = Memory::new(bytes.clone(), 1);
        input.failure_kind = kind;
        input.fail_open = true;
        let polls = input.polls.clone();
        let accessor = WarcAccessor::new(input);
        let error = accessor
            .open(ReadRequest::new("good_resource.warc"))
            .await
            .err()
            .unwrap();
        assert_original_source_error(&error, kind);
        assert_eq!(polls.load(Ordering::SeqCst), 0);

        // Faults while reading a header, body, and terminator must remain source
        // errors. Unlike malformed-file EOF, they are not decoder failures.
        for offset in [0, body_start + 1, bytes.len() - 3] {
            let mut input = Memory::new(bytes.clone(), 1);
            input.failure_kind = kind;
            input.fail_at = Some(offset);
            let polls = input.polls.clone();
            let accessor = WarcAccessor::new(input);
            let mut reader = accessor
                .open(ReadRequest::new("good_resource.warc"))
                .await
                .unwrap();
            let error = reader.next().await.err().unwrap();
            assert_wrapped_source_error(&error, kind);
            let calls = polls.load(Ordering::SeqCst);
            assert!(reader.next().await.unwrap().is_none());
            assert!(reader.next().await.unwrap().is_none());
            assert_eq!(polls.load(Ordering::SeqCst), calls);
        }
    }
}

#[tokio::test]
async fn empty_source_chunks_are_io_errors_and_terminal() {
    let mut input = Memory::new(fixture("good_resource.warc"), 1);
    input.empty_at = Some(0);
    let polls = input.polls.clone();
    let accessor = WarcAccessor::new(input);
    let mut reader = accessor
        .open(ReadRequest::new("good_resource.warc"))
        .await
        .unwrap();
    let error = reader.next().await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::Io);
    assert_eq!(error.operation(), "decode_warc");
    let source = error.source().unwrap();
    assert_eq!(ErrorKind::from_source(source), Some(ErrorKind::Io));
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(polls.load(Ordering::SeqCst), 1);
}

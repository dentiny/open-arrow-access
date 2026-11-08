use std::{collections::VecDeque, sync::Arc};

use arrow_array::RecordBatchOptions;
use futures::stream;
use open_arrow_access_core::{
    BatchDecoder, ByteMetadata, ByteReader, Bytes, Error, ErrorKind, Reader, RecordBatch,
    RecordMetadata, Result, Schema, StreamReader, decode_batches,
};
use open_arrow_access_test_support::fixture;

struct Decoder {
    steps: VecDeque<(&'static str, usize, bool)>,
    ready: bool,
    remaining: usize,
}

impl Decoder {
    fn new(steps: impl IntoIterator<Item = (&'static str, usize, bool)>) -> Self {
        Self {
            steps: steps.into_iter().collect(),
            ready: false,
            remaining: 2,
        }
    }
}

impl BatchDecoder for Decoder {
    const INPUT_OPERATION: &'static str = "decode_fixture";

    fn is_ready(&self) -> bool {
        self.ready
    }

    fn decode(&mut self, bytes: &[u8]) -> Result<usize> {
        let (expected, consumed, ready) = self.steps.pop_front().expect("unexpected decode call");
        assert_eq!(bytes, expected.as_bytes());
        self.ready = ready;
        Ok(consumed)
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>> {
        self.ready = false;
        Ok(Some(
            RecordBatch::try_new_with_options(
                Arc::new(Schema::empty()),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(1)),
            )
            .unwrap(),
        ))
    }

    fn finish(&mut self) -> Result<Option<RecordBatch>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        self.flush()
    }
}

fn input(chunks: Vec<Result<Bytes>>) -> Box<dyn ByteReader> {
    Box::new(StreamReader::new(
        stream::iter(chunks),
        ByteMetadata::default(),
    ))
}

fn metadata() -> RecordMetadata {
    RecordMetadata {
        schema: Arc::new(Schema::empty()),
        source: None,
    }
}

#[tokio::test]
async fn preserves_pending_input_and_drains_batches_at_eof() {
    // A decoder may emit a batch without consuming the pending bytes.
    let decoder = Decoder::new([
        ("abc", 1, false),
        ("bc", 0, true),
        ("bc", 2, false),
        ("d", 1, false),
    ]);
    let bytes = Bytes::from(fixture!("bytes", "decoder.bin").await);
    let mut reader = decode_batches(
        input(vec![Ok(bytes.slice(..3)), Ok(bytes.slice(3..))]),
        decoder,
        metadata(),
    );
    for _ in 0..3 {
        assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
    }
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn rejects_invalid_consumption_and_no_progress() {
    for (consumed, message) in [(2, "more bytes"), (0, "no progress")] {
        let mut reader = decode_batches(
            input(vec![Ok(Bytes::from(
                fixture!("bytes", "decoder_invalid.bin").await,
            ))]),
            Decoder::new([("x", consumed, false)]),
            metadata(),
        );
        let error = reader.next().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Decode);
        assert_eq!(error.operation(), "decode_fixture");
        assert!(error.to_string().contains(message));
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn rejects_empty_chunks_and_preserves_source_errors() {
    for (chunk, kind, operation) in [
        (Ok(Bytes::new()), ErrorKind::Io, "decode_fixture"),
        (
            Err(Error::new(
                ErrorKind::PermissionDenied,
                "read_fixture",
                "denied",
            )),
            ErrorKind::PermissionDenied,
            "read_fixture",
        ),
    ] {
        let mut reader = decode_batches(input(vec![chunk]), Decoder::new([]), metadata());
        let error = reader.next().await.unwrap_err();
        assert_eq!(error.kind(), kind);
        assert_eq!(error.operation(), operation);
        assert!(reader.next().await.unwrap().is_none());
    }
}

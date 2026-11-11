use arrow_json::reader::Decoder;
use arrow_schema::ArrowError;
use bytes::BytesMut;
use futures::stream;
use open_arrow_access_core::{
    BatchDecoder, ByteReader, OrderRequirement, RecordBatch, RecordMetadata, RecordReader, Result,
    StreamReader, decode_batches,
};

use super::{error::decode_error, nonfinite::normalize_nonfinite, planning::JsonFrames};

struct JsonDecoder {
    decoder: Decoder,
    batch_size: usize,
    eof_flushed: bool,
}

impl BatchDecoder for JsonDecoder {
    const INPUT_OPERATION: &'static str = "read_json";
    const EMPTY_CHUNK_MESSAGE: &'static str = "byte reader emitted an empty chunk";

    fn is_ready(&self) -> bool {
        self.decoder.len() == self.batch_size && !self.decoder.has_partial_record()
    }

    fn decode(&mut self, bytes: &[u8]) -> Result<usize> {
        self.decoder.decode(bytes).map_err(decode_error)
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>> {
        self.decoder.flush().map_err(decode_error)
    }

    fn finish(&mut self) -> Result<Option<RecordBatch>> {
        if self.eof_flushed {
            return Ok(None);
        }
        self.eof_flushed = true;
        self.flush()
    }
}

pub(super) fn json_reader(
    input: Box<dyn ByteReader>,
    decoder: Decoder,
    metadata: RecordMetadata,
    batch_size: usize,
) -> impl RecordReader {
    // Frame one object at a time so normalization also handles fragmented byte input.
    let byte_metadata = input.metadata().clone();
    let frames = JsonFrames {
        input,
        pending_input: BytesMut::new(),
    };
    let stream = stream::try_unfold(frames, |mut frames| async move {
        frames
            .next_chunk(/*row_target=*/ 1, /*byte_target=*/ usize::MAX)
            .await
            .map(|bytes| {
                bytes.map(|bytes| {
                    let bytes = match normalize_nonfinite(&bytes) {
                        std::borrow::Cow::Borrowed(_) => bytes,
                        std::borrow::Cow::Owned(normalized) => normalized.into(),
                    };
                    (bytes, frames)
                })
            })
            .map_err(|error| {
                if error.operation() == "frame_json" {
                    decode_error(ArrowError::JsonError(error.to_string()))
                } else {
                    error
                }
            })
    });
    decode_batches(
        Box::new(StreamReader::new(stream, byte_metadata)),
        JsonDecoder {
            decoder,
            batch_size,
            eof_flushed: false,
        },
        metadata,
        OrderRequirement::Reorderable,
    )
}

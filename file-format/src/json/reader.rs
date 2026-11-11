use arrow_json::reader::Decoder;
use open_arrow_access_core::{
    BatchDecoder, ByteReader, OrderRequirement, RecordBatch, RecordMetadata, RecordReader, Result,
    decode_batches,
};

use super::error::decode_error;

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
    decode_batches(
        input,
        JsonDecoder {
            decoder,
            batch_size,
            eof_flushed: false,
        },
        metadata,
        OrderRequirement::Reorderable,
    )
}

use arrow_csv::reader::Decoder;
use open_arrow_access_core::{ByteReader, RecordBatch, RecordMetadata, RecordReader, Result};

use super::decode_error;
use crate::decode::{BatchDecoder, decode_reader};

impl BatchDecoder for Decoder {
    const INPUT_OPERATION: &'static str = "read_csv";

    fn is_ready(&self) -> bool {
        self.capacity() == 0
    }

    fn decode(&mut self, bytes: &[u8]) -> Result<usize> {
        Decoder::decode(self, bytes).map_err(decode_error)
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>> {
        Decoder::flush(self).map_err(decode_error)
    }

    fn finish(&mut self) -> Result<Option<RecordBatch>> {
        // Arrow CSV needs an empty decode to finalize a row without a newline.
        Decoder::decode(self, &[]).map_err(decode_error)?;
        Decoder::flush(self).map_err(decode_error)
    }
}

pub(super) fn csv_reader(
    input: Box<dyn ByteReader>,
    decoder: Decoder,
    metadata: RecordMetadata,
) -> impl RecordReader {
    decode_reader(input, decoder, metadata)
}

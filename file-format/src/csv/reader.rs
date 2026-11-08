use arrow_csv::reader::Decoder;
use open_arrow_access_core::{
    BatchDecoder, ByteReader, RecordBatch, RecordMetadata, RecordReader, Result, decode_batches,
};

use super::decode_error;

struct CsvDecoder(Decoder);

impl BatchDecoder for CsvDecoder {
    const INPUT_OPERATION: &'static str = "read_csv";

    fn is_ready(&self) -> bool {
        self.0.capacity() == 0
    }

    fn decode(&mut self, bytes: &[u8]) -> Result<usize> {
        self.0.decode(bytes).map_err(decode_error)
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>> {
        self.0.flush().map_err(decode_error)
    }

    fn finish(&mut self) -> Result<Option<RecordBatch>> {
        // Arrow CSV needs an empty decode to finalize a row without a newline.
        self.0.decode(&[]).map_err(decode_error)?;
        self.0.flush().map_err(decode_error)
    }
}

pub(super) fn csv_reader(
    input: Box<dyn ByteReader>,
    decoder: Decoder,
    metadata: RecordMetadata,
) -> impl RecordReader {
    decode_batches(input, CsvDecoder(decoder), metadata)
}

use arrow_json::reader::Decoder;
use open_arrow_access_core::{
    BatchDecoder, ByteReader, OrderRequirement, RecordBatch, RecordMetadata, RecordReader, Result,
    SchemaRef, decode_batches, project_batch,
};

use super::error::decode_error;

struct JsonDecoder {
    decoder: Decoder,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    schema: SchemaRef,
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
        self.decoder
            .flush()
            .map_err(decode_error)?
            .map(|batch| project_batch(batch, self.projection.as_deref(), self.schema.clone()))
            .transpose()
    }

    fn finish(&mut self) -> Result<Option<RecordBatch>> {
        self.flush()
    }
}

pub(super) fn json_reader(
    input: Box<dyn ByteReader>,
    decoder: Decoder,
    projection: Option<Vec<usize>>,
    metadata: RecordMetadata,
    batch_size: usize,
) -> impl RecordReader {
    decode_batches(
        super::nonfinite::normalize_input(input),
        JsonDecoder {
            decoder,
            batch_size,
            projection,
            schema: metadata.schema.clone(),
        },
        metadata,
        OrderRequirement::Reorderable,
    )
}

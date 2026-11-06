use arrow_json::reader::Decoder;
use open_arrow_access_core::{
    ByteReader, Bytes, Capability, Error, ErrorKind, Reader, RecordBatch, RecordMetadata, Result,
    async_trait,
};

use super::decode_error;

// TODO: Decode JSONL chunks in parallel at complete line boundaries, preserving
// input order and retaining sequential decoding for general JSON object streams.
pub(super) struct JsonReader {
    input: Box<dyn ByteReader>,
    decoder: Decoder,
    pending: Bytes,
    metadata: RecordMetadata,
    batch_size: usize,
    finished: bool,
}

impl JsonReader {
    pub(super) fn new(
        input: Box<dyn ByteReader>,
        decoder: Decoder,
        metadata: RecordMetadata,
        batch_size: usize,
    ) -> Self {
        Self {
            input,
            decoder,
            pending: Bytes::new(),
            metadata,
            batch_size,
            finished: false,
        }
    }

    async fn read_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if self.decoder.len() == self.batch_size && !self.decoder.has_partial_record() {
                return self.decoder.flush().map_err(decode_error);
            }
            if self.pending.is_empty() {
                match self.input.next().await? {
                    Some(bytes) if bytes.is_empty() => {
                        return Err(Error::new(
                            ErrorKind::Io,
                            "read_json",
                            "byte reader emitted an empty chunk",
                        ));
                    }
                    Some(bytes) => self.pending = bytes,
                    None => {
                        self.finished = true;
                        return self.decoder.flush().map_err(decode_error);
                    }
                }
            }
            let consumed = self.decoder.decode(&self.pending).map_err(decode_error)?;
            self.pending = self.pending.slice(consumed..);
        }
    }
}

#[async_trait]
impl Reader for JsonReader {
    type Item = RecordBatch;
    type Metadata = RecordMetadata;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            list: false,
        }
    }

    fn metadata(&self) -> &RecordMetadata {
        &self.metadata
    }

    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.finished {
            return Ok(None);
        }
        let result = self.read_batch().await;
        if !matches!(result, Ok(Some(_))) {
            self.finished = true;
        }
        result
    }
}

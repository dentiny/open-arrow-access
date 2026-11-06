use arrow_csv::reader::Decoder;
use open_arrow_access_core::{
    ByteReader, Bytes, Capability, Error, ErrorKind, Reader, RecordBatch, RecordMetadata, Result,
    async_trait,
};

use super::decode_error;

pub(super) struct CsvReader {
    input: Box<dyn ByteReader>,
    decoder: Decoder,
    metadata: RecordMetadata,
    /// Bytes read from the input but not yet consumed by the decoder.
    pending: Bytes,
    /// The input has ended; the decoder may still have a final batch to emit.
    eof: bool,
    /// Reading has terminated after exhaustion or an error; subsequent calls return `Ok(None)`.
    finished: bool,
}

impl CsvReader {
    pub(super) fn new(
        input: Box<dyn ByteReader>,
        decoder: Decoder,
        metadata: RecordMetadata,
    ) -> Self {
        Self {
            input,
            decoder,
            metadata,
            pending: Bytes::new(),
            eof: false,
            finished: false,
        }
    }

    async fn read_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if self.decoder.capacity() == 0 {
                return self.decoder.flush().map_err(decode_error);
            }
            if self.pending.is_empty() && !self.eof {
                match self.input.next().await? {
                    Some(bytes) if bytes.is_empty() => {
                        return Err(Error::new(
                            ErrorKind::Io,
                            "read_csv",
                            "byte reader returned an empty chunk",
                        ));
                    }
                    Some(bytes) => self.pending = bytes,
                    None => self.eof = true,
                }
            }
            let consumed = self.decoder.decode(&self.pending).map_err(decode_error)?;
            self.pending = self.pending.slice(consumed..);
            if self.eof {
                return self.decoder.flush().map_err(decode_error);
            }
        }
    }
}

#[async_trait]
impl Reader for CsvReader {
    type Item = RecordBatch;
    type Metadata = RecordMetadata;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            ..Capability::default()
        }
    }

    fn metadata(&self) -> &RecordMetadata {
        &self.metadata
    }

    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.finished {
            return Ok(None);
        }
        let batch = self.read_batch().await;
        if !matches!(batch, Ok(Some(_))) {
            self.finished = true;
        }
        batch
    }
}

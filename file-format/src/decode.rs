use futures::stream;
use open_arrow_access_core::{
    ByteReader, Bytes, Error, ErrorKind, RecordBatch, RecordMetadata, RecordReader, Result,
    StreamReader,
};

/// Format-specific decoding hooks for the shared input loop.
pub(crate) trait BatchDecoder: Send + 'static {
    /// Operation name for empty-chunk input errors.
    const INPUT_OPERATION: &'static str;

    /// Message for empty-chunk input errors.
    const EMPTY_CHUNK_MESSAGE: &'static str = "byte reader returned an empty chunk";

    /// Whether a batch is ready to flush without more input.
    fn is_ready(&self) -> bool;

    /// Consume a prefix of `bytes`; return its length and retain partial records.
    fn decode(&mut self, bytes: &[u8]) -> Result<usize>;

    /// Emit buffered records. `Ok(None)` ends output.
    fn flush(&mut self) -> Result<Option<RecordBatch>>;

    /// Apply EOF rules and drain remaining batches until `Ok(None)`.
    fn finish(&mut self) -> Result<Option<RecordBatch>>;
}

struct DecodeState<D> {
    input: Box<dyn ByteReader>,
    decoder: D,
    pending: Bytes,
    input_eof: bool,
}

impl<D: BatchDecoder> DecodeState<D> {
    async fn read_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if self.decoder.is_ready() {
                return self.decoder.flush();
            }
            if self.pending.is_empty() {
                if self.input_eof {
                    return self.decoder.finish();
                }
                match self.input.next().await? {
                    Some(bytes) if bytes.is_empty() => {
                        return Err(Error::new(
                            ErrorKind::Io,
                            D::INPUT_OPERATION,
                            D::EMPTY_CHUNK_MESSAGE,
                        ));
                    }
                    Some(bytes) => self.pending = bytes,
                    None => {
                        self.input_eof = true;
                        continue;
                    }
                }
            }
            let consumed = self.decoder.decode(&self.pending)?;
            self.pending = self.pending.slice(consumed..);
        }
    }
}

pub(crate) fn create_reader<D: BatchDecoder>(
    input: Box<dyn ByteReader>,
    decoder: D,
    metadata: RecordMetadata,
) -> impl RecordReader {
    let state = DecodeState {
        input,
        decoder,
        pending: Bytes::new(),
        input_eof: false,
    };
    let stream = stream::try_unfold(state, |mut state| async move {
        Ok(state.read_batch().await?.map(|batch| (batch, state)))
    });
    StreamReader::new(stream, metadata)
}

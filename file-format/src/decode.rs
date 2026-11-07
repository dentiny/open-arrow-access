use futures::stream;
use open_arrow_access_core::{
    ByteReader, Bytes, Error, ErrorKind, RecordBatch, RecordMetadata, RecordReader, Result,
    StreamReader,
};

/// Private bridge between the upstream incremental decoders and the shared input loop.
pub(crate) trait BatchDecoder: Send + 'static {
    /// Operation name attached to the driver's empty-chunk I/O error.
    /// Errors returned by the upstream byte reader keep their original operation.
    const INPUT_OPERATION: &'static str;

    /// Diagnostic for an upstream byte reader that emits an empty chunk instead
    /// of EOF. Override to retain the format's input-error message.
    const EMPTY_CHUNK_MESSAGE: &'static str = "byte reader returned an empty chunk";

    /// Whether a complete batch can be flushed without consuming more input.
    /// Checked before each decode or input read; account for partial records so
    /// the driver does not flush an incomplete record as a full batch.
    fn is_ready(&self) -> bool;

    /// Feed the current nonempty input slice and return its consumed prefix
    /// length, at most `bytes.len()`. Retain partial-record state internally;
    /// the driver retains the unconsumed suffix. Repeated calls must make
    /// progress or become ready. Map parser failures to format-specific errors.
    fn decode(&mut self, bytes: &[u8]) -> Result<usize>;

    /// Emit buffered records without reading more input. The driver calls this
    /// when [`Self::is_ready`] is true; [`Self::finish`] may also flush a short
    /// final batch. `Ok(None)` ends output rather than requesting more input.
    /// Return decoding failures with the format's operation and original cause.
    fn flush(&mut self) -> Result<Option<RecordBatch>>;

    /// Finalize after input EOF and consumption of all pending bytes, applying
    /// the decoder's EOF rules before emitting remaining records. After a batch,
    /// this may be called again to drain output without reading more input;
    /// do not emit the same final batch twice. `Ok(None)` or an error terminates
    /// output. Report incomplete or malformed final records as decoding errors.
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

pub(crate) fn incremental_reader<D: BatchDecoder>(
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

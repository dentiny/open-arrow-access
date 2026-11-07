use atif_arrow::TrajectoryBatchBuilder;
use futures::stream;
use open_arrow_access_core::{
    ByteReader, Bytes, ObjectRef, RecordBatch, RecordMetadata, RecordReader, Result, StreamReader,
};

use super::{
    AtifFormat,
    error::{decode_error, empty_chunk_error},
};

struct AtifDecoder {
    input: Box<dyn ByteReader>,
    format: AtifFormat,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    source: Option<ObjectRef>,
    pending: Bytes,
    document: Vec<u8>,
    record_index: u64,
    input_eof: bool,
}

impl AtifDecoder {
    async fn read_document(&mut self) -> Result<Option<String>> {
        loop {
            if self.input_eof && self.document.is_empty() {
                return Ok(None);
            }
            if self.pending.is_empty() && !self.input_eof {
                match self.input.next().await? {
                    Some(bytes) if bytes.is_empty() => {
                        return Err(empty_chunk_error());
                    }
                    Some(bytes) => self.pending = bytes,
                    None => self.input_eof = true,
                }
            }
            let newline = match self.format {
                AtifFormat::Json => None,
                AtifFormat::JsonLines => self.pending.iter().position(|byte| *byte == b'\n'),
            };
            let consumed = newline.map_or(self.pending.len(), |index| index + 1);
            self.document.extend_from_slice(&self.pending[..consumed]);
            self.pending = self.pending.slice(consumed..);
            if newline.is_some() || self.input_eof {
                let document =
                    String::from_utf8(std::mem::take(&mut self.document)).map_err(|error| {
                        decode_error(self.source.as_ref(), self.record_index, error)
                    })?;
                if self.format == AtifFormat::JsonLines && document.trim().is_empty() {
                    continue;
                }
                return Ok(Some(document));
            }
        }
    }

    async fn read_batch(&mut self) -> Result<Option<RecordBatch>> {
        let mut builder = TrajectoryBatchBuilder::new(self.batch_size)
            .map_err(|error| decode_error(self.source.as_ref(), self.record_index, error))?;
        for _ in 0..self.batch_size {
            let Some(document) = self.read_document().await? else {
                break;
            };
            builder
                .append_json(
                    &document,
                    self.source.as_ref().map(|source| source.locator()),
                    self.record_index,
                )
                .map_err(|error| decode_error(self.source.as_ref(), self.record_index, error))?;
            self.record_index += 1;
        }
        builder
            .flush()
            .map_err(|error| decode_error(self.source.as_ref(), self.record_index, error))?
            .map(|batch| match &self.projection {
                None => Ok(batch),
                Some(indices) => batch
                    .project(indices)
                    .map_err(|error| decode_error(self.source.as_ref(), self.record_index, error)),
            })
            .transpose()
    }
}

/// Create an ATIF reader with shared metadata and terminal-state handling.
pub(super) fn create_reader(
    input: Box<dyn ByteReader>,
    format: AtifFormat,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    metadata: RecordMetadata,
) -> impl RecordReader {
    let decoder = AtifDecoder {
        input,
        format,
        batch_size,
        projection,
        source: metadata.source.clone(),
        pending: Bytes::new(),
        document: Vec::new(),
        record_index: 1,
        input_eof: false,
    };
    let stream = stream::try_unfold(decoder, |mut decoder| async move {
        Ok(decoder.read_batch().await?.map(|batch| (batch, decoder)))
    });
    StreamReader::new(stream, metadata)
}

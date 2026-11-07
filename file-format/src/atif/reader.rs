use atif_arrow::TrajectoryBatchBuilder;
use open_arrow_access_core::{
    ByteReader, Bytes, Capability, ReadState, Reader, RecordBatch, RecordMetadata, Result,
    async_trait,
};

use super::{
    AtifFormat,
    error::{decode_error, empty_chunk_error},
};

pub(super) struct AtifReader {
    input: Box<dyn ByteReader>,
    format: AtifFormat,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    metadata: RecordMetadata,
    pending: Bytes,
    document: Vec<u8>,
    record_index: u64,
    state: ReadState,
}

impl AtifReader {
    pub(super) fn new(
        input: Box<dyn ByteReader>,
        format: AtifFormat,
        batch_size: usize,
        projection: Option<Vec<usize>>,
        metadata: RecordMetadata,
    ) -> Self {
        Self {
            input,
            format,
            batch_size,
            projection,
            metadata,
            pending: Bytes::new(),
            document: Vec::new(),
            record_index: 1,
            state: ReadState::default(),
        }
    }

    async fn read_document(&mut self) -> Result<Option<String>> {
        loop {
            if self.state.input_eof() && self.document.is_empty() {
                return Ok(None);
            }
            if self.pending.is_empty() && !self.state.input_eof() {
                match self.input.next().await? {
                    Some(bytes) if bytes.is_empty() => {
                        return Err(empty_chunk_error());
                    }
                    Some(bytes) => self.pending = bytes,
                    None => self.state.mark_input_eof(),
                }
            }
            let newline = match self.format {
                AtifFormat::Json => None,
                AtifFormat::JsonLines => self.pending.iter().position(|byte| *byte == b'\n'),
            };
            let consumed = newline.map_or(self.pending.len(), |index| index + 1);
            self.document.extend_from_slice(&self.pending[..consumed]);
            self.pending = self.pending.slice(consumed..);
            if newline.is_some() || self.state.input_eof() {
                let document =
                    String::from_utf8(std::mem::take(&mut self.document)).map_err(|error| {
                        decode_error(self.metadata.source.as_ref(), self.record_index, error)
                    })?;
                if self.format == AtifFormat::JsonLines && document.trim().is_empty() {
                    continue;
                }
                return Ok(Some(document));
            }
        }
    }

    async fn read_batch(&mut self) -> Result<Option<RecordBatch>> {
        let mut builder = TrajectoryBatchBuilder::new(self.batch_size).map_err(|error| {
            decode_error(self.metadata.source.as_ref(), self.record_index, error)
        })?;
        for _ in 0..self.batch_size {
            let Some(document) = self.read_document().await? else {
                break;
            };
            builder
                .append_json(
                    &document,
                    self.metadata.source.as_ref().map(|source| source.locator()),
                    self.record_index,
                )
                .map_err(|error| {
                    decode_error(self.metadata.source.as_ref(), self.record_index, error)
                })?;
            self.record_index += 1;
        }
        builder
            .flush()
            .map_err(|error| decode_error(self.metadata.source.as_ref(), self.record_index, error))?
            .map(|batch| match &self.projection {
                None => Ok(batch),
                Some(indices) => batch.project(indices).map_err(|error| {
                    decode_error(self.metadata.source.as_ref(), self.record_index, error)
                }),
            })
            .transpose()
    }
}

#[async_trait]
impl Reader for AtifReader {
    type Item = RecordBatch;
    type Metadata = RecordMetadata;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    fn metadata(&self) -> &RecordMetadata {
        &self.metadata
    }

    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.state.is_finished() {
            return Ok(None);
        }
        let batch = self.read_batch().await;
        self.state.observe(&batch);
        batch
    }
}

use atif_arrow::TrajectoryBatchBuilder;
use futures::{StreamExt, TryStreamExt, stream, stream::BoxStream};
use open_arrow_access_core::{
    ByteReader, Bytes, ObjectRef, RecordBatch, RecordMetadata, RecordReader, Result, StreamReader,
    read_limited, read_lines,
};

use super::{AtifFormat, error::decode_error};

struct AtifDecoder {
    documents: BoxStream<'static, Result<Bytes>>,
    format: AtifFormat,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    source: Option<ObjectRef>,
    record_index: u64,
}

impl AtifDecoder {
    async fn read_document(&mut self) -> Result<Option<String>> {
        while let Some(bytes) = self.documents.try_next().await? {
            let document = String::from_utf8(bytes.into())
                .map_err(|error| decode_error(self.source.as_ref(), self.record_index, error))?;
            if self.format == AtifFormat::JsonLines && document.trim().is_empty() {
                continue;
            }
            return Ok(Some(document));
        }
        Ok(None)
    }

    async fn read_batch(&mut self) -> Result<Option<RecordBatch>> {
        let mut builder = match &self.projection {
            Some(indices) => TrajectoryBatchBuilder::new_with_projection(self.batch_size, indices),
            None => TrajectoryBatchBuilder::new(self.batch_size),
        }
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
            .map_err(|error| decode_error(self.source.as_ref(), self.record_index, error))
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
    let documents = match format {
        AtifFormat::Json => stream::once(async move {
            read_limited(input, usize::MAX, "read_atif")
                .await
                .map(Bytes::from)
        })
        .boxed(),
        AtifFormat::JsonLines => read_lines(input, None, "read_atif").boxed(),
    };
    let decoder = AtifDecoder {
        documents,
        format,
        batch_size,
        projection,
        source: metadata.source.clone(),
        record_index: 1,
    };
    let stream = stream::try_unfold(decoder, |mut decoder| async move {
        Ok(decoder.read_batch().await?.map(|batch| (batch, decoder)))
    });
    StreamReader::new(stream, metadata)
}

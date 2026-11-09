use atif_arrow::TrajectoryBatchBuilder;
use futures::{StreamExt, TryStreamExt, stream, stream::BoxStream};
use open_arrow_access_core::{
    ByteReader, Bytes, ObjectRef, RecordBatch, RecordMetadata, RecordReader, Result, StreamReader,
    read_all, read_lines,
};

use super::{AtifFormat, error::decode_error};

struct AtifDecoder {
    documents: BoxStream<'static, Result<Bytes>>,
    format: AtifFormat,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    builder: Option<TrajectoryBatchBuilder>,
    source: Option<ObjectRef>,
    record_index: u64,
}

impl AtifDecoder {
    async fn read_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.builder.is_none() {
            self.builder = Some(
                TrajectoryBatchBuilder::new(self.batch_size, self.projection.as_deref()).map_err(
                    |error| decode_error(self.source.as_ref(), self.record_index, error),
                )?,
            );
        }
        let builder = self.builder.as_mut().unwrap();
        let mut appended = 0;
        while appended < self.batch_size {
            let Some(bytes) = self.documents.try_next().await? else {
                break;
            };
            // Keep shared line slices borrowed through conversion. Turning Bytes
            // into a Vec/String can copy the entire document first.
            let document = std::str::from_utf8(&bytes)
                .map_err(|error| decode_error(self.source.as_ref(), self.record_index, error))?;
            if self.format == AtifFormat::JsonLines && document.trim().is_empty() {
                continue;
            }
            builder
                .append_json(
                    document,
                    self.source.as_ref().map(|source| source.locator()),
                    self.record_index,
                )
                .map_err(|error| decode_error(self.source.as_ref(), self.record_index, error))?;
            self.record_index += 1;
            appended += 1;
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
        AtifFormat::Json => {
            stream::once(async move { read_all(input, "read_atif").await.map(Bytes::from) }).boxed()
        }
        AtifFormat::JsonLines => read_lines(input, None, "read_atif").boxed(),
    };
    let decoder = AtifDecoder {
        documents,
        format,
        batch_size,
        projection,
        builder: None,
        source: metadata.source.clone(),
        record_index: 1,
    };
    let stream = stream::try_unfold(decoder, |mut decoder| async move {
        Ok(decoder.read_batch().await?.map(|batch| (batch, decoder)))
    });
    StreamReader::new(stream, metadata)
}

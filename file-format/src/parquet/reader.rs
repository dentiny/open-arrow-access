use futures::StreamExt;
use open_arrow_access_core::{BoxFuture, Capability, Reader, RecordBatch, RecordMetadata, Result};
use parquet::arrow::async_reader::ParquetRecordBatchStream;

use super::{decode_error, input::ParquetInput};

/// Decode ordered Arrow batches while preserving the inspected output schema.
pub(super) struct ParquetReader {
    stream: ParquetRecordBatchStream<ParquetInput>,
    metadata: RecordMetadata,
    reorder: Option<Vec<usize>>,
    finished: bool,
}

impl ParquetReader {
    /// Initialize record consumption with the inspected schema and requested field order.
    pub(super) fn new(
        stream: ParquetRecordBatchStream<ParquetInput>,
        metadata: RecordMetadata,
        reorder: Option<Vec<usize>>,
    ) -> Self {
        Self {
            stream,
            metadata,
            reorder,
            finished: false,
        }
    }
}

impl Reader for ParquetReader {
    type Item = RecordBatch;
    type Metadata = RecordMetadata;

    /// Report record consumption without exposing the decoder's internal byte ranges.
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            ..Capability::default()
        }
    }

    /// Borrow the fixed output schema and source selected during opening.
    fn metadata(&self) -> &RecordMetadata {
        &self.metadata
    }

    /// Decode the next ordered batch, apply field order and restore schema metadata.
    /// Stop reading permanently after EOF or the first decoding/projection failure.
    fn next(&mut self) -> BoxFuture<'_, Result<Option<RecordBatch>>> {
        Box::pin(async move {
            if self.finished {
                return Ok(None);
            }
            let result = match self.stream.next().await {
                Some(Ok(batch)) => match self.reorder.as_ref() {
                    Some(indices) => batch
                        .project(indices)
                        .map(Some)
                        .map_err(|e| decode_error("project", e)),
                    None => Ok(Some(batch)),
                },
                Some(Err(error)) => Err(decode_error("decode_parquet", error)),
                None => Ok(None),
            };
            let result = result.and_then(|batch| {
                batch
                    .map(|batch| batch.with_schema(self.metadata.schema.clone()))
                    .transpose()
                    .map_err(|e| decode_error("restore_schema", e))
            });
            if !matches!(&result, Ok(Some(_))) {
                self.finished = true;
            }
            result
        })
    }
}

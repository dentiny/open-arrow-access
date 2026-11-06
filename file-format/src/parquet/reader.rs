use futures::StreamExt;
use open_arrow_access_core::{
    Capability, Reader, RecordBatch, RecordMetadata, Result, async_trait,
};
use parquet::arrow::async_reader::ParquetRecordBatchStream;

use super::{decode_error, input::ParquetInput};

/// Decode ordered Arrow batches while preserving the inspected output schema.
// TODO: Read and decode row groups concurrently with bounded concurrency,
// preserving row order and the fixed output schema.
pub(super) struct ParquetReader {
    stream: ParquetRecordBatchStream<ParquetInput>,
    metadata: RecordMetadata,
    reorder: Option<Vec<usize>>,
    finished: bool,
}

impl ParquetReader {
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

#[async_trait]
impl Reader for ParquetReader {
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
    }
}

use futures::StreamExt;
use open_arrow_access_core::{RecordMetadata, RecordReader, StreamReader};
use parquet::arrow::async_reader::ParquetRecordBatchStream;

use super::{decode_error, input::ParquetInput};

/// Decode ordered Arrow batches while preserving the inspected output schema.
// TODO: Read and decode row groups concurrently with bounded concurrency,
// preserving row order and the fixed output schema.
pub(super) fn parquet_reader(
    stream: ParquetRecordBatchStream<ParquetInput>,
    metadata: RecordMetadata,
    reorder: Option<Vec<usize>>,
) -> impl RecordReader {
    let schema = metadata.schema.clone();
    let stream = stream.map(move |result| {
        let batch = result.map_err(|error| decode_error("decode_parquet", error))?;
        let batch = match &reorder {
            Some(indices) => batch
                .project(indices)
                .map_err(|error| decode_error("project", error))?,
            None => batch,
        };
        batch
            .with_schema(schema.clone())
            .map_err(|error| decode_error("restore_schema", error))
    });
    StreamReader::new(stream, metadata)
}

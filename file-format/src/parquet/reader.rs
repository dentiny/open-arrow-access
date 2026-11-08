use futures::StreamExt;
use open_arrow_access_core::{RecordMetadata, RecordReader, StreamReader, project_batch};
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
        project_batch(batch, reorder.as_deref(), schema.clone())
    });
    StreamReader::new(stream, metadata)
}

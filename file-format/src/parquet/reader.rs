use futures::StreamExt;
use open_arrow_access_core::{RecordMetadata, RecordReader, record_stream_reader};
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
    let stream = stream.map(|result| result.map_err(|error| decode_error("decode_parquet", error)));
    record_stream_reader(stream, metadata, reorder)
}

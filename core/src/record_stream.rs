use futures::{Stream, StreamExt};

use crate::{RecordBatch, RecordMetadata, RecordReader, Result, StreamReader, project_batch};

/// Adapt native Arrow batches to a sequential reader with a fixed output schema.
///
/// Project/reorder columns and attach `metadata.schema`, sharing arrays and
/// retaining field/schema metadata. `None` keeps all columns; an empty projection
/// preserves row counts with zero columns.
///
/// Map parser errors to [`crate::Error`] first. Source errors pass through;
/// projection/schema errors become decode errors. [`StreamReader`] handles
/// lazy construction and terminal cleanup.
pub fn record_stream_reader<S>(
    stream: S,
    metadata: RecordMetadata,
    projection: Option<Vec<usize>>,
) -> impl RecordReader
where
    S: Stream<Item = Result<RecordBatch>> + Send,
{
    let schema = metadata.schema.clone();
    let stream =
        stream.map(move |result| project_batch(result?, projection.as_deref(), schema.clone()));
    StreamReader::new(stream, metadata)
}

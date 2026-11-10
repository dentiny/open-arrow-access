use futures::{Stream, StreamExt};

use crate::{RecordBatch, RecordMetadata, RecordReader, Result, StreamReader, project_batch};

/// Project native Arrow batches and attach `metadata.schema` without copying arrays.
/// `None` keeps all columns; an empty projection preserves only row counts.
/// Source errors pass through; projection/schema failures become decode errors.
/// [`StreamReader`] supplies sequential reading and terminal cleanup.
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

use futures::{Stream, StreamExt};

use crate::{RecordBatch, RecordMetadata, RecordReader, Result, StreamReader, project_batch};

/// Adapt native Arrow batches to a sequential reader with a fixed output schema.
///
/// Project or reorder each batch, then attach `metadata.schema`, retaining schema
/// and field metadata. `None` retains every column; an empty projection retains
/// row counts with zero columns. Arrays are shared without copying.
///
/// Map parser-specific errors into [`crate::Error`] before calling this helper.
/// Source errors are preserved; projection/schema failures are decode errors.
/// Construction does not poll the stream. EOF and errors release the stream and
/// terminate reading, following [`StreamReader`]'s lifecycle.
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

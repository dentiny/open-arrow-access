use futures::{Stream, StreamExt};

use crate::{
    OrderRequirement, RecordBatch, RecordCapability, RecordMetadata, RecordReader, Result,
    SchemaRef, StreamReader, into_stream, project_batch,
};

/// Project native Arrow batches and attach `metadata.schema` without copying arrays.
/// `None` keeps all columns; an empty projection preserves only row counts.
/// Source errors pass through; projection/schema failures become decode errors.
/// `ordering` is the source contract, independent of the requested output policy.
/// [`StreamReader`] supplies sequential reading and terminal cleanup.
pub fn record_stream_reader<S>(
    stream: S,
    metadata: RecordMetadata,
    projection: Option<Vec<usize>>,
    ordering: OrderRequirement,
) -> impl RecordReader
where
    S: Stream<Item = Result<RecordBatch>> + Send,
{
    let schema = metadata.schema.clone();
    let stream =
        stream.map(move |result| project_batch(result?, projection.as_deref(), schema.clone()));
    StreamReader::records(stream, metadata, ordering)
}

/// Map each input batch to exactly one output batch, retaining the input source.
/// The caller supplies the output schema and capabilities and must ensure the
/// transform produces matching batches and exposes only supported operations.
/// Input schema validation and identity fast paths belong to the caller.
/// Construction does not poll input; source and transform errors pass through.
/// EOF, errors and dropping the reader release the input and transform state.
pub fn map_record_reader<F>(
    reader: Box<dyn RecordReader>,
    schema: SchemaRef,
    capability: RecordCapability,
    mut transform: F,
) -> impl RecordReader
where
    F: FnMut(RecordBatch) -> Result<RecordBatch> + Send,
{
    let metadata = RecordMetadata {
        schema,
        source: reader.metadata().source.clone(),
    };
    let batches = into_stream(reader).map(move |batch| transform(batch?));
    StreamReader::records(batches, metadata, capability.ordering).with_capability(capability)
}

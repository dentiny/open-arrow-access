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

/// Apply a row-preserving batch transform while retaining each row's coordinate.
/// Plans and other transform state are captured once when the reader opens.
/// A transform that changes the row count fails before exposing that batch.
pub fn map_positioned_reader<F>(
    reader: Box<dyn crate::PositionedRecordReader>,
    schema: SchemaRef,
    capability: RecordCapability,
    mut transform: F,
) -> impl crate::PositionedRecordReader
where
    F: FnMut(RecordBatch) -> Result<RecordBatch> + Send,
{
    let metadata = RecordMetadata {
        schema,
        source: reader.metadata().source.clone(),
    };
    let batches = into_stream(reader).map(move |batch| {
        let (batch, positions) = batch?.into_parts();
        crate::PositionedBatch::try_new(transform(batch)?, positions)
    });
    StreamReader::records(batches, metadata, capability.ordering).with_capability(capability)
}

/// Require an opened reader's metadata and every batch to match a fixed schema.
///
/// Schema equality includes field and schema metadata. Metadata is checked
/// immediately without polling input; batches are checked lazily. The error
/// factory supplies the category and operation for mismatches. Capabilities and
/// source metadata are retained, and lazy errors gain the reader's source context
/// just like [`map_record_reader`]. EOF or any error releases the input and makes
/// later reads return EOF. Upstream error categories and sources pass through.
pub fn schema_checked_reader<F>(
    reader: Box<dyn RecordReader>,
    schema: SchemaRef,
    schema_error: F,
) -> Result<Box<dyn RecordReader>>
where
    F: Fn() -> crate::Error + Send + 'static,
{
    crate::validate_schema(&reader.metadata().schema, &schema, &schema_error)?;
    let capability = reader.capability();
    Ok(Box::new(map_record_reader(
        reader,
        schema.clone(),
        capability,
        move |batch| {
            crate::validate_schema(&batch.schema(), &schema, &schema_error)?;
            Ok(batch)
        },
    )))
}

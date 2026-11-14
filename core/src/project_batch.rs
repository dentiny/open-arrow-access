use crate::{RecordBatch, Result, SchemaRef};

/// Project or reorder columns and attach the supplied output schema.
/// `None` retains all columns; an empty projection preserves the row count.
/// Arrays are shared without copying. Failures return [`crate::ErrorKind::Decode`].
pub fn project_batch(
    batch: RecordBatch,
    projection: Option<&[usize]>,
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let project = || {
        let batch = match projection {
            Some(indices) => batch.project(indices)?,
            None => batch,
        };
        batch.with_schema(schema)
    };
    project().map_err(crate::error::project_batch_error)
}

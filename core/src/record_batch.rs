use arrow_array::{ArrayRef, RecordBatchOptions};
use arrow_schema::ArrowError;

use crate::{RecordBatch, SchemaRef};

/// Assemble a batch with an explicit row count, including zero-column batches.
/// Arrays are shared without copying. Arrow validates the schema and column lengths;
/// its errors are returned unchanged so callers can supply their own error context.
///
/// ```
/// use std::sync::Arc;
/// use open_arrow_access_core::{Schema, record_batch_with_rows};
///
/// let batch = record_batch_with_rows(Arc::new(Schema::empty()), vec![], 3)?;
/// assert_eq!(batch.num_columns(), 0);
/// assert_eq!(batch.num_rows(), 3);
/// # Ok::<(), arrow_schema::ArrowError>(())
/// ```
pub fn record_batch_with_rows(
    schema: SchemaRef,
    columns: Vec<ArrayRef>,
    rows: usize,
) -> std::result::Result<RecordBatch, ArrowError> {
    RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
}

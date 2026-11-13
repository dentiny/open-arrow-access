use std::collections::HashMap;

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{ArrowError, SchemaRef};

pub(crate) fn assemble_batch(
    schema: &SchemaRef,
    indices: &[usize],
    source: &RecordBatch,
    // Arrays produced by preceding add/cast steps, keyed by the slots in `indices`.
    // Missing entries read from `source`; added-column slots must have an entry.
    values: &HashMap<usize, ArrayRef>,
) -> std::result::Result<RecordBatch, ArrowError> {
    let columns = indices
        .iter()
        .map(|&index| {
            values
                .get(&index)
                .unwrap_or_else(|| source.column(index))
                .clone()
        })
        .collect();
    RecordBatch::try_new_with_options(
        schema.clone(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(source.num_rows())),
    )
}

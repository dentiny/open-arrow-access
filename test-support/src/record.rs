//! Helpers for comparing record-reader output.

use arrow_array::{Array, cast::AsArray, types::ArrowPrimitiveType};
use futures::TryStreamExt;
use open_arrow_access_core::{RecordBatch, RecordReader, into_stream};

/// Collect a non-null primitive column in batch order.
pub fn collect_values<T: ArrowPrimitiveType>(
    batches: &[RecordBatch],
    column: usize,
) -> Vec<T::Native> {
    let mut values = Vec::new();
    for batch in batches {
        let array = batch.column(column).as_primitive::<T>();
        assert_eq!(
            array.null_count(),
            0,
            "collect_values requires non-null values"
        );
        values.extend_from_slice(array.values());
    }
    values
}

/// Read all batches and collect a non-null primitive column.
pub async fn collect_column<T: ArrowPrimitiveType>(
    reader: Box<dyn RecordReader>,
    column: usize,
) -> Vec<T::Native> {
    let batches = into_stream(reader).try_collect::<Vec<_>>().await.unwrap();
    collect_values::<T>(&batches, column)
}

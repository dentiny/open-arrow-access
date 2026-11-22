use super::*;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatchOptions, StringViewArray};
use arrow_schema::Schema;
use open_arrow_access_core::{OrderRequirement, RecordMetadata, StreamReader};

fn n(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn batch(start: i64, len: i64) -> RecordBatch {
    RecordBatch::try_from_iter([(
        "id",
        Arc::new(Int64Array::from_iter_values(start..start + len)) as ArrayRef,
    )])
    .unwrap()
}

fn reader(items: Vec<Result<RecordBatch>>, settings: CoalesceBatchesLayer) -> Coalescer {
    let schema = items[0].as_ref().unwrap().schema();
    Coalescer {
        schema: schema.clone(),
        inner: Some(Box::new(StreamReader::records(
            stream::iter(items),
            RecordMetadata {
                schema,
                source: None,
            },
            OrderRequirement::PreserveGlobal,
        ))),
        pending: None,
        error: None,
        settings,
    }
}

#[tokio::test]
async fn combines_small_batches_and_slices_large_inputs_without_copying_the_tail() {
    let large = batch(2, 100);
    let mut input = reader(
        vec![Ok(batch(0, 2)), Ok(large.clone())],
        CoalesceBatchesLayer::rows(n(4)),
    );
    assert_eq!(input.next().await.unwrap().unwrap(), batch(0, 4));
    let tail = input.next().await.unwrap().unwrap();
    assert_eq!(tail, batch(4, 4));
    let original = large
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let values = tail
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.values().as_ptr(), original.values()[2..].as_ptr());
    assert_eq!(input.pending.as_ref().unwrap().num_rows(), 94);
}

#[tokio::test]
async fn huge_row_ceiling_allocates_for_actual_rows_and_flushes_before_source_error() {
    let mut input = reader(
        vec![
            Ok(batch(0, 1)),
            Ok(batch(1, 2)),
            Err(Error::new(ErrorKind::Io, "test", "source failed")),
        ],
        CoalesceBatchesLayer::rows(n(usize::MAX)),
    );
    assert_eq!(input.next().await.unwrap().unwrap(), batch(0, 3));
    assert!(input.next().await.is_err());
    assert!(input.next().await.unwrap().is_none());
}

#[tokio::test]
async fn byte_budget_flushes_before_the_row_ceiling() {
    let one = batch(0, 1);
    let mut input = reader(
        vec![Ok(one.clone()), Ok(batch(1, 1)), Ok(batch(2, 1))],
        CoalesceBatchesLayer::rows(n(10))
            .with_max_estimated_bytes(n(one.get_array_memory_size() * 2)),
    );
    assert_eq!(input.next().await.unwrap().unwrap(), batch(0, 2));
    assert_eq!(input.next().await.unwrap().unwrap(), batch(2, 1));
    assert!(input.next().await.unwrap().is_none());
}

#[tokio::test]
async fn merging_preserves_schema_metadata_and_zero_column_rows() {
    let schema = Arc::new(
        Schema::empty().with_metadata([("source".into(), "test".into())].into_iter().collect()),
    );
    let empty = |rows| {
        RecordBatch::try_new_with_options(
            schema.clone(),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .unwrap()
    };
    let mut input = reader(
        vec![Ok(empty(0)), Ok(empty(2)), Ok(empty(3))],
        CoalesceBatchesLayer::rows(n(10)),
    );
    let result = input.next().await.unwrap().unwrap();
    assert_eq!(result.num_rows(), 5);
    assert_eq!(result.num_columns(), 0);
    assert!(Arc::ptr_eq(&schema, &result.schema()));
}

#[tokio::test]
async fn sparse_views_keep_their_backing_buffers() {
    let text = "a long string stored outside the inline view".repeat(100);
    let values = StringViewArray::from_iter_values([text.as_str(), "another long string"]);
    let sparse = values.slice(1, 1);
    let batch = RecordBatch::try_from_iter([("text", Arc::new(sparse) as ArrayRef)]).unwrap();
    let mut input = reader(
        vec![Ok(batch.clone()), Ok(batch)],
        CoalesceBatchesLayer::rows(n(2)),
    );
    let result = input.next().await.unwrap().unwrap();
    let output = result
        .column(0)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .unwrap();
    assert_eq!(output.len(), 2);
    assert_eq!(
        output.data_buffers()[0].as_ptr(),
        values.data_buffers()[0].as_ptr()
    );
}

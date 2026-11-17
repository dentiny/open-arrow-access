use std::sync::{Arc, atomic::Ordering};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, types::Int64Type};
use open_arrow_access_core::{Accessor, AccessorExt, ReadRequest};
use open_arrow_access_layers::LimitLayer;
use open_arrow_access_test_support::{MemoryRecordAccessor, record::collect_column};

#[tokio::test]
async fn limit_keeps_first_rows_across_batches() {
    let batch = RecordBatch::try_from_iter([(
        "id",
        Arc::new(Int64Array::from_iter_values(0..10)) as ArrayRef,
    )])
    .unwrap();
    let source = MemoryRecordAccessor::new(vec![
        batch.slice(0, 3),
        batch.slice(3, 4),
        batch.slice(7, 3),
    ]);
    let polls = source.polls.clone();
    let records = source.layer(LimitLayer::new(5));
    let reader = records.open(ReadRequest::new("input")).await.unwrap();

    assert_eq!(
        collect_column::<Int64Type>(reader, 0).await,
        [0, 1, 2, 3, 4]
    );
    assert_eq!(polls.load(Ordering::SeqCst), 2);
}

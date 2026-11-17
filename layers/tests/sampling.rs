use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, types::Int64Type};
use open_arrow_access_core::{Accessor, AccessorExt, ReadRequest};
use open_arrow_access_layers::SamplingLayer;
use open_arrow_access_test_support::{MemoryRecordAccessor, record::collect_column};

#[tokio::test]
async fn sampling_rate_one_keeps_every_row() {
    let batch = RecordBatch::try_from_iter([(
        "id",
        Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4])) as ArrayRef,
    )])
    .unwrap();
    let source = MemoryRecordAccessor::new(vec![
        batch.slice(0, 2),
        batch.slice(2, 2),
        batch.slice(4, 1),
    ])
    .layer(SamplingLayer::try_new(1.0, 42).unwrap());
    let reader = source.open(ReadRequest::new("input")).await.unwrap();
    assert_eq!(
        collect_column::<Int64Type>(reader, 0).await,
        [0, 1, 2, 3, 4]
    );
}

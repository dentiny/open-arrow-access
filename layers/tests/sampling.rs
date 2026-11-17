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

#[tokio::test]
async fn transformed_sampling_is_lazy_and_independent_of_batch_boundaries() {
    use std::sync::atomic::Ordering;

    let batch = RecordBatch::try_from_iter([(
        "id",
        Arc::new(Int64Array::from_iter_values(0..64)) as ArrayRef,
    )])
    .unwrap();
    let zero = MemoryRecordAccessor::new(vec![batch.clone()]);
    let polls = zero.polls.clone();
    let zero = zero.layer(SamplingLayer::try_new(0.0, 42).unwrap());
    let reader = zero.open(ReadRequest::new("input")).await.unwrap();
    assert!(collect_column::<Int64Type>(reader, 0).await.is_empty());
    assert_eq!(polls.load(Ordering::SeqCst), 0);

    let sample = SamplingLayer::try_new(0.5, 42).unwrap();
    let whole = MemoryRecordAccessor::new(vec![batch.clone()]).layer(sample);
    let fragmented = MemoryRecordAccessor::new(vec![
        batch.slice(0, 7),
        batch.slice(7, 31),
        batch.slice(38, 26),
    ]);
    let polls = fragmented.polls.clone();
    let fragmented = fragmented.layer(sample);
    let whole = whole.open(ReadRequest::new("input")).await.unwrap();
    let fragmented = fragmented.open(ReadRequest::new("input")).await.unwrap();
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    let whole = collect_column::<Int64Type>(whole, 0).await;
    assert!(!whole.is_empty() && whole.len() < 64);
    assert_eq!(collect_column::<Int64Type>(fragmented, 0).await, whole);
}

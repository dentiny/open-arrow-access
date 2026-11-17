use std::{collections::HashMap, sync::Arc};

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures::{StreamExt, stream};
use open_arrow_access_core::{OrderRequirement, Reader, RecordMetadata, record_stream_reader};

fn batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("left", DataType::Int32, false),
            Field::new("right", DataType::Int32, false),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![3, 4])),
        ],
    )
    .unwrap()
}

#[rstest::rstest]
#[case::full(None)]
#[case::reordered(Some(vec![1, 0]))]
#[case::empty(Some(vec![]))]
#[tokio::test]
async fn restores_schema_and_field_metadata_for_full_reordered_and_empty_projections(
    #[case] projection: Option<Vec<usize>>,
) {
    let batch = batch();
    let full = Schema::new_with_metadata(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| {
                field
                    .as_ref()
                    .clone()
                    .with_metadata(HashMap::from([("unit".into(), "count".into())]))
            })
            .collect::<Vec<_>>(),
        HashMap::from([("version".into(), "fixture".into())]),
    );
    let schema = Arc::new(match &projection {
        Some(indices) => full.project(indices).unwrap(),
        None => full.clone(),
    });
    let mut reader = record_stream_reader(
        stream::iter([Ok(batch.clone())]).then(|item| async move { item }),
        RecordMetadata {
            schema: schema.clone(),
            source: Some("native.xml".into()),
        },
        projection.clone(),
        OrderRequirement::PreserveGlobal,
    );
    assert_eq!(reader.metadata().schema, schema);
    assert_eq!(
        reader.metadata().source.as_ref().unwrap().locator(),
        "native.xml"
    );
    let output = reader.next().await.unwrap().unwrap();
    assert_eq!(output.schema(), schema);
    assert_eq!(output.num_rows(), 2);
    let indices = projection.unwrap_or_else(|| vec![0, 1]);
    for (output_index, input_index) in indices.into_iter().enumerate() {
        assert!(Arc::ptr_eq(
            output.column(output_index),
            batch.column(input_index)
        ));
    }
    assert!(reader.next().await.unwrap().is_none());
}

#[rstest::rstest]
#[case::reorderable(OrderRequirement::Reorderable)]
#[case::global(OrderRequirement::PreserveGlobal)]
#[tokio::test]
async fn memory_source_and_projected_readers_share_ordering(#[case] ordering: OrderRequirement) {
    use std::sync::atomic::Ordering;

    use open_arrow_access_core::{Accessor, ReadOptions, ReadRequest, RecordCapability};
    use open_arrow_access_test_support::MemoryRecordAccessor;

    let batch = batch();
    let source = MemoryRecordAccessor::new(vec![batch.clone()]);
    assert_eq!(source.capability().ordering, OrderRequirement::Reorderable);
    let source = source.with_ordering(ordering);
    let cloned = source.clone();
    let expected = RecordCapability::sequential(ordering);
    assert_eq!(source.capability(), expected);
    assert_eq!(cloned.capability(), expected);
    let mut projected = cloned
        .open(ReadRequest::new("projected").with_options(ReadOptions {
            projection: Some(vec!["right".into()]),
            ..Default::default()
        }))
        .await
        .unwrap();
    let mut full = source.open(ReadRequest::new("full")).await.unwrap();
    assert_eq!(projected.capability(), expected);
    assert_eq!(full.capability(), expected);
    assert_eq!(source.polls.load(Ordering::SeqCst), 0);
    assert_eq!(
        *source.projections.lock().unwrap(),
        [Some(vec!["right".into()]), None]
    );
    assert_eq!(
        projected.next().await.unwrap().unwrap(),
        batch.project(&[1]).unwrap()
    );
    assert_eq!(full.next().await.unwrap().unwrap(), batch);
    assert!(projected.next().await.unwrap().is_none());
    assert!(full.next().await.unwrap().is_none());
    assert!(projected.next().await.unwrap().is_none());
    assert_eq!(source.polls.load(Ordering::SeqCst), 4);
    assert_eq!(source.batches_read.load(Ordering::SeqCst), 2);
}

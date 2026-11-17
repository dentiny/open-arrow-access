use std::{collections::HashMap, sync::Arc};

use arrow_array::RecordBatchOptions;
use futures::stream;
use open_arrow_access_core::{
    OrderRequirement, RecordBatch, RecordMetadata, RecordReader, Schema, record_stream_reader,
};
use open_arrow_access_test_support::record::assert_same_batches;

fn reader(rows: &[usize], metadata: &[(&str, &str)]) -> Box<dyn RecordReader> {
    let schema = Arc::new(
        Schema::empty().with_metadata(
            metadata
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect::<HashMap<_, _>>(),
        ),
    );
    let batches = rows
        .iter()
        .map(|rows| {
            Ok(RecordBatch::try_new_with_options(
                schema.clone(),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(*rows)),
            )
            .unwrap())
        })
        .collect::<Vec<_>>();
    Box::new(record_stream_reader(
        stream::iter(batches),
        RecordMetadata {
            schema,
            source: None,
        },
        None,
        OrderRequirement::PreserveGlobal,
    ))
}

#[tokio::test]
async fn equal_batches_include_zero_columns_and_empty_batches() {
    for rows in [&[][..], &[0, 2, 1][..]] {
        assert_same_batches(reader(rows, &[]).as_mut(), reader(rows, &[]).as_mut()).await;
    }
}

#[tokio::test]
#[should_panic]
async fn equal_rows_with_different_batch_boundaries_fail() {
    assert_same_batches(reader(&[2], &[]).as_mut(), reader(&[1, 1], &[]).as_mut()).await;
}

#[tokio::test]
#[should_panic]
async fn extra_actual_batch_fails() {
    assert_same_batches(reader(&[1], &[]).as_mut(), reader(&[1, 0], &[]).as_mut()).await;
}

#[tokio::test]
#[should_panic]
async fn extra_expected_batch_fails() {
    assert_same_batches(reader(&[1, 0], &[]).as_mut(), reader(&[1], &[]).as_mut()).await;
}

#[tokio::test]
#[should_panic]
async fn schema_metadata_must_match_even_without_batches() {
    assert_same_batches(
        reader(&[], &[("source", "expected")]).as_mut(),
        reader(&[], &[("source", "actual")]).as_mut(),
    )
    .await;
}

use std::{collections::HashMap, sync::Arc};

use arrow_array::{ArrayRef, GenericStringArray, OffsetSizeTrait, RecordBatchOptions, StringArray};
use futures::stream;
use open_arrow_access_core::{
    Accessor, OrderRequirement, ReadRequest, RecordBatch, RecordMetadata, RecordReader, Schema,
    record_stream_reader,
};
use open_arrow_access_test_support::record::{
    MemoryRecordAccessor, assert_same_batches, collect_nullable_strings, collect_strings, column,
    field_names,
};

async fn string_layout<O: OffsetSizeTrait>() {
    let batch = RecordBatch::try_from_iter([(
        "text",
        Arc::new(GenericStringArray::<O>::from(vec![
            Some(""),
            None,
            Some("你好"),
        ])) as ArrayRef,
    )])
    .unwrap();
    assert_eq!(
        collect_nullable_strings::<O>(&[batch.clone(), batch.slice(2, 1)], 0),
        [
            Some("".into()),
            None,
            Some("你好".into()),
            Some("你好".into())
        ]
    );
    assert_eq!(
        column::<GenericStringArray<O>>(&batch, "text").value(2),
        "你好"
    );
    let reader = MemoryRecordAccessor::new(vec![batch.slice(0, 1), batch.slice(2, 1)])
        .open(ReadRequest::new("strings"))
        .await
        .unwrap();
    assert_eq!(collect_strings::<O>(reader, 0).await, ["", "你好"]);
}

#[tokio::test]
async fn string_layouts_preserve_nulls_empty_values_slices_and_batch_order() {
    string_layout::<i32>().await;
    string_layout::<i64>().await;
}

#[tokio::test]
#[should_panic(expected = "collect_strings requires non-null values")]
async fn nonnull_strings_reject_nulls() {
    let batch = RecordBatch::try_from_iter([(
        "text",
        Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
    )])
    .unwrap();
    let reader = MemoryRecordAccessor::new(vec![batch])
        .open(ReadRequest::new("null"))
        .await
        .unwrap();
    collect_strings::<i32>(reader, 0).await;
}

#[test]
fn field_names_keep_order_and_duplicates() {
    let batch = RecordBatch::try_from_iter([
        (
            "same",
            Arc::new(StringArray::from(vec!["first"])) as ArrayRef,
        ),
        (
            "same",
            Arc::new(StringArray::from(vec!["second"])) as ArrayRef,
        ),
    ])
    .unwrap();
    assert_eq!(
        field_names(&Schema::new(batch.schema().fields().clone())),
        ["same", "same"]
    );
}

#[test]
#[should_panic]
fn string_collectors_require_the_requested_offset_width() {
    let batch = RecordBatch::try_from_iter([(
        "text",
        Arc::new(StringArray::from(vec!["utf8"])) as ArrayRef,
    )])
    .unwrap();
    collect_nullable_strings::<i64>(&[batch], 0);
}

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

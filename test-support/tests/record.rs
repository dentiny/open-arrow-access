use std::sync::Arc;

use arrow_array::{ArrayRef, GenericStringArray, OffsetSizeTrait, StringArray};
use open_arrow_access_core::{Accessor, ReadRequest, RecordBatch, Schema};
use open_arrow_access_test_support::record::{
    MemoryRecordAccessor, collect_nullable_strings, collect_strings, column, field_names,
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

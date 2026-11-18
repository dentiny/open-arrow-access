use std::{num::NonZeroU64, sync::Arc};

use arrow_array::{ArrayRef, Int64Array, builder::StringDictionaryBuilder, types::Int8Type};
use open_arrow_access_core::{RecordBatch, Schema};

use super::*;

fn batch() -> RecordBatch {
    let mut dictionary = StringDictionaryBuilder::<Int8Type>::new();
    dictionary.append("alpha").unwrap();
    dictionary.append_null();
    dictionary.append("βeta").unwrap();
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
        ("dictionary", Arc::new(dictionary.finish()) as ArrayRef),
    ])
    .unwrap();
    let schema = Arc::new(Schema::new_with_metadata(
        batch.schema().fields().clone(),
        [("source".into(), "fixture".into())].into(),
    ));
    RecordBatch::try_new(schema, batch.columns().to_vec()).unwrap()
}

#[tokio::test]
async fn spill_round_trips_dictionaries_and_metadata_while_appending_and_unlinks() {
    let expected = batch();
    let (mut writer, mut reader) = SpillWriter::new(NonZeroU64::new(1024 * 1024).unwrap())
        .await
        .unwrap();
    let path = reader._temporary.path().to_owned();
    assert!(writer.append(&expected).await.unwrap());
    assert_eq!(reader.next().await.unwrap(), Some(expected.clone()));
    let task = tokio::spawn(async move {
        assert_eq!(reader.next().await.unwrap(), Some(batch()));
        assert!(reader.next().await.unwrap().is_none());
    });
    assert!(writer.append(&expected).await.unwrap());
    writer.finish();
    task.await.unwrap();
    assert!(!path.exists());
}

#[tokio::test]
async fn spill_quota_includes_framing_and_never_writes_an_over_budget_batch() {
    let batch = batch();
    let (mut writer, reader) = SpillWriter::new(NonZeroU64::new(1024 * 1024).unwrap())
        .await
        .unwrap();
    assert!(writer.append(&batch).await.unwrap());
    let size = writer.written;
    writer.finish();
    drop(reader);
    let (mut writer, mut reader) = SpillWriter::new(NonZeroU64::new(size).unwrap())
        .await
        .unwrap();
    assert!(writer.append(&batch).await.unwrap());
    assert!(!writer.append(&batch).await.unwrap());
    assert_eq!(writer.file.metadata().await.unwrap().len(), size);
    writer.finish();
    assert_eq!(reader.next().await.unwrap(), Some(batch));
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn zero_column_batches_keep_their_row_counts() {
    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(7)),
    )
    .unwrap();
    let (mut writer, mut reader) = SpillWriter::new(NonZeroU64::new(4096).unwrap())
        .await
        .unwrap();
    assert!(writer.append(&batch).await.unwrap());
    writer.finish();
    assert_eq!(reader.next().await.unwrap(), Some(batch));
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn spool_read_errors_are_reported_and_the_temporary_file_is_removed() {
    let (mut writer, mut reader) = SpillWriter::new(NonZeroU64::new(1024 * 1024).unwrap())
        .await
        .unwrap();
    let path = reader._temporary.path().to_owned();
    assert!(writer.append(&batch()).await.unwrap());
    writer.file.set_len(8).await.unwrap();
    writer.finish();
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Io);
    drop(reader);
    assert!(!path.exists());
}

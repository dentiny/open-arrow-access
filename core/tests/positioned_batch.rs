use std::sync::Arc;

use arrow_array::RecordBatchOptions;
use futures::stream;
use open_arrow_access_core::{
    BytePosition, ErrorKind, OrderRequirement, PositionedBatch, PositionedRecordReader,
    RecordBatch, RecordMetadata, Schema, StreamReader, map_positioned_reader,
};

fn empty_columns(rows: usize) -> RecordBatch {
    RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .unwrap()
}

#[test]
fn coordinates_must_match_rows_even_without_columns() {
    for count in [1, 2, 3] {
        let result =
            PositionedBatch::try_new(empty_columns(2), vec![BytePosition::Stored(10); count]);
        if count == 2 {
            let positioned = result.unwrap();
            assert_eq!(positioned.batch().num_rows(), 2);
            assert_eq!(positioned.positions().len(), 2);
        } else {
            assert_eq!(result.unwrap_err().kind(), ErrorKind::Decode);
        }
    }
}

#[tokio::test]
async fn row_changing_mapping_is_terminal_before_misaligned_coordinates_escape() {
    let batch = PositionedBatch::try_new(
        empty_columns(2),
        vec![BytePosition::Stored(10), BytePosition::Stored(20)],
    )
    .unwrap();
    let reader: Box<dyn PositionedRecordReader> = Box::new(StreamReader::records(
        stream::iter([Ok(batch)]),
        RecordMetadata {
            schema: Arc::new(Schema::empty()),
            source: Some("source".into()),
        },
        OrderRequirement::Reorderable,
    ));
    let capability = reader.capability();
    let mut reader =
        map_positioned_reader(reader, Arc::new(Schema::empty()), capability, |batch| {
            Ok(batch.slice(0, 1))
        });
    use open_arrow_access_core::Reader;
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Decode);
    assert!(error.to_string().contains("source"));
    assert!(reader.next().await.unwrap().is_none());
}

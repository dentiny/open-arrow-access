use super::*;
use crate::FileSetRequest;
use arrow_array::{Array, LargeBinaryArray, UInt64Array};
use futures::TryStreamExt;
use open_arrow_access_core::{Accessor, ReadOptions, ScanPlanner};
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::temp_fs;

#[tokio::test]
async fn pages_preserve_bytes_identity_supplied_metadata_and_projection() {
    let item = "ocr/item-000001/item-000001-00000003-0002.txt";
    let part = "ocr/part-000008/part-000008-00000003-0001.txt";
    let (_root, operator) = temp_fs(&[(item, b"\r\n\xff"), (part, b"")]);
    let pages = BhlPageAccessor::new(
        OpenDalAccessor::new(operator),
        "aws-inventory-id",
        |identity| {
            Ok(super::super::page::tests::metadata(
                &format!("Page {}", identity.page_id),
                "Source rights",
            ))
        },
    )
    .unwrap();
    let plan = pages
        .plan_scan(FileSetRequest::new([item, part]))
        .await
        .unwrap();
    let partitions = plan.partitions.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(partitions.len(), 2);
    for (index, partition) in partitions.into_iter().enumerate() {
        let mut reader = partition.await.unwrap();
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        let data = batch
            .column_by_name("data")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert_eq!(
            data.value(0),
            if index == 0 {
                b"\r\n\xff".as_slice()
            } else {
                b"".as_slice()
            }
        );
        let id = batch
            .column_by_name("entity_id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(id.value(0), if index == 0 { 1 } else { 8 });
        assert!(!batch.column_by_name("metadata").unwrap().is_null(0));
        assert!(
            batch
                .column_by_name("source_entry_index")
                .unwrap()
                .is_null(0)
        );
        assert!(reader.next().await.unwrap().is_none());
    }
    let options = ReadOptions::default().with_projection(["metadata", "page_id"]);
    let mut reader = pages
        .open(FileSetRequest::new([item]).with_options(options))
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().num_columns(), 2);
    assert!(
        pages
            .plan_scan(FileSetRequest::new(["ocr/item-1/page-1.txt"]))
            .await
            .is_err()
    );
}

#[test]
fn schema_declares_all_fields_and_identity_rejects_invalid_numbers() {
    let schema = page_schema();
    let arrow_schema::DataType::Struct(tables) =
        schema.field_with_name("metadata").unwrap().data_type()
    else {
        panic!("metadata must be a struct")
    };
    assert_eq!(tables.len(), 13);
    let fields = tables
        .iter()
        .map(|table| {
            let data_type = match table.data_type() {
                arrow_schema::DataType::List(item) => item.data_type(),
                data_type => data_type,
            };
            let arrow_schema::DataType::Struct(fields) = data_type else {
                panic!("table must be a struct")
            };
            fields.len()
        })
        .sum::<usize>();
    assert_eq!(fields, 104);
    for key in [
        "ocr/item-1/part-1-3-1.txt",
        "ocr/item-1/item-2-3-1.txt",
        "ocr/item-1/item-1-0-1.txt",
        "ocr/item-1/item-1-3-18446744073709551616.txt",
    ] {
        assert!(BhlPageIdentity::parse(key).is_err());
    }
}

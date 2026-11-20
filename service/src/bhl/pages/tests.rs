use super::*;
use crate::FileSetRequest;
use arrow_array::{Array, LargeBinaryArray, UInt64Array};
use futures::TryStreamExt;
use open_arrow_access_core::{Accessor, ByteReadOptions, ErrorKind, ReadOptions, ScanPlanner};
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::{MemoryAccessor, temp_fs};
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[tokio::test]
async fn projections_without_data_never_open_ocr_and_keep_rows_and_provenance() {
    let storage = MemoryAccessor::new(b"unread OCR", 3).with_open_failure(ErrorKind::NotFound);
    let observed = storage.observations.clone();
    let supplied = super::super::page::tests::metadata("Source title", "Source rights");
    let source_metadata = supplied.clone();
    let pages = BhlPageAccessor::new(storage, "snapshot", move |_| Ok(source_metadata.clone()))
        .unwrap()
        .with_schema_metadata(std::collections::HashMap::from([(
            "bhl.metadata.fingerprint".into(),
            "fingerprint".into(),
        )]));
    let keys = ["ocr/item-1/item-1-3-2.txt", "ocr/part-8/part-8-4-1.txt"];
    for columns in [
        vec!["page_id"],
        vec!["metadata", "page_id"],
        vec![],
        vec![
            "snapshot_id",
            "source_path",
            "sequence",
            "entity_id",
            "entity_type",
            "source_entry_index",
        ],
    ] {
        let mut reader = pages
            .open(
                FileSetRequest::new(keys)
                    .with_options(ReadOptions::default().with_projection(columns.clone())),
            )
            .await
            .unwrap();
        for (index, key) in keys.iter().enumerate() {
            let batch = reader.next().await.unwrap().unwrap();
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>(),
                columns
            );
            assert_eq!(
                batch.schema().metadata()["bhl.metadata.fingerprint"],
                "fingerprint"
            );
            if let Some(id) = batch.column_by_name("page_id") {
                assert_eq!(
                    id.as_any().downcast_ref::<UInt64Array>().unwrap().value(0),
                    3 + index as u64
                );
            }
            if let Some(metadata) = batch.column_by_name("metadata") {
                assert_eq!(
                    metadata
                        .as_any()
                        .downcast_ref::<arrow_array::StructArray>()
                        .unwrap(),
                    &supplied
                );
            }
            if let Some(path) = batch.column_by_name("source_path") {
                assert!(
                    path.as_any()
                        .downcast_ref::<arrow_array::LargeStringArray>()
                        .unwrap()
                        .value(0)
                        .ends_with(key)
                );
            }
        }
        assert!(reader.next().await.unwrap().is_none());
    }
    assert!(observed.requests.lock().unwrap().is_empty());
    assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
    let mut reader = pages
        .open(
            FileSetRequest::new([keys[0]])
                .with_options(ReadOptions::default().with_projection(["data"])),
        )
        .await
        .unwrap();
    assert!(reader.next().await.is_err());
    assert_eq!(observed.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn selecting_data_preserves_original_bytes_and_io_options() {
    for bytes in [b"\r\n\xff".as_slice(), b""] {
        let storage = MemoryAccessor::new(bytes, 1);
        let observed = storage.observations.clone();
        let pages = BhlPageAccessor::new(storage, "snapshot", |_| {
            Ok(super::super::page::tests::metadata("Title", "Rights"))
        })
        .unwrap();
        let options = ByteReadOptions {
            io_chunk_size: NonZeroUsize::new(64 * 1024 * 1024),
            ..ByteReadOptions::default()
        };
        let mut reader = pages
            .open(
                FileSetRequest::new(["ocr/item-1/item-1-3-2.txt"])
                    .with_byte_options(options)
                    .with_options(ReadOptions::default().with_projection(["data", "page_id"])),
            )
            .await
            .unwrap();
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.schema().field(0).name(), "data");
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .value(0),
            bytes
        );
        assert!(reader.next().await.unwrap().is_none());
        let requests = observed.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].options, options);
    }
}

#[tokio::test]
async fn invalid_projections_fail_before_metadata_or_ocr_work() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let pages = BhlPageAccessor::new(MemoryAccessor::new(b"OCR", 3), "snapshot", move |_| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(super::super::page::tests::metadata("Title", "Rights"))
    })
    .unwrap();
    for fields in [vec!["missing"], vec!["data", "data"]] {
        assert!(
            pages
                .plan_scan(
                    FileSetRequest::new(["ocr/item-1/item-1-3-2.txt"])
                        .with_options(ReadOptions::default().with_projection(fields))
                )
                .await
                .is_err()
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

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
    .unwrap()
    .with_schema_metadata(std::collections::HashMap::from([(
        "bhl.metadata.fingerprint".into(),
        "fixture-fingerprint".into(),
    )]));
    let plan = pages
        .plan_scan(FileSetRequest::new([item, part]))
        .await
        .unwrap();
    let partitions = plan.partitions.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(partitions.len(), 2);
    for (index, partition) in partitions.into_iter().enumerate() {
        let mut reader = partition.await.unwrap();
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(
            batch.schema().metadata()["bhl.metadata.fingerprint"],
            "fixture-fingerprint"
        );
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

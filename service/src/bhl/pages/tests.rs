use super::*;
use crate::FileSetRequest;
use arrow_array::{Array, LargeBinaryArray, UInt64Array};
use futures::TryStreamExt;
use open_arrow_access_core::{Accessor, ReadOptions, Reader, ScanPlanner};
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

#[tokio::test]
async fn duckdb_pages_stream_ocr_metadata_and_reports() {
    use arrow_array::cast::AsArray;
    use open_arrow_access_test_support::MemoryAccessor;
    use std::sync::{Arc, Mutex};
    let metadata = Arc::new(super::super::metadata::tests::load().await);
    let bytes = MemoryAccessor::new(b"\r\n\xff", 2);
    let observations = bytes.observations.clone();
    let reports = Arc::new(Mutex::new(Vec::new()));
    let captured = reports.clone();
    let pages = BhlPageAccessor::from_metadata(bytes, "inventory", metadata.clone(), move |page| {
        captured
            .lock()
            .unwrap()
            .push((page.object.locator().to_owned(), page.missing.clone()));
        Ok(())
    })
    .unwrap()
    .with_schema_metadata(std::collections::HashMap::from([(
        "caller".into(),
        "value".into(),
    )]));
    let item = "ocr/item-2/item-2-3-5.txt";
    let part = "ocr/part-8/part-8-3-1.txt";
    let missing = "ocr/item-20/item-20-30-1.txt";
    let byte_options = open_arrow_access_core::ByteReadOptions {
        io_concurrency: std::num::NonZeroUsize::new(2).unwrap(),
        io_chunk_size: std::num::NonZeroUsize::new(2),
    };
    let plan = pages
        .plan_scan(FileSetRequest::new([item, part, item, missing]).with_byte_options(byte_options))
        .await
        .unwrap();
    assert!(plan.streaming_discovery);
    assert!(observations.requests.lock().unwrap().is_empty());
    assert!(reports.lock().unwrap().is_empty());
    let schema = plan.metadata.schema.clone();
    assert_eq!(
        schema.metadata()["bhl.metadata.fingerprint"],
        metadata.info.fingerprint
    );
    assert_eq!(schema.metadata()["caller"], "value");
    let mut reader = plan.into_reader();
    let mut count = 0;
    let mut ids = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.schema(), schema);
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch["data"].as_binary::<i64>().value(0), b"\r\n\xff");
        let joined = batch["metadata"].as_struct();
        if batch["page_id"]
            .as_primitive::<arrow_array::types::UInt64Type>()
            .value(0)
            == 3
        {
            assert_eq!(
                joined
                    .column_by_name("title")
                    .unwrap()
                    .as_struct()
                    .column_by_name("TitleID")
                    .unwrap()
                    .as_string::<i64>()
                    .value(0),
                "001"
            );
        } else {
            assert!(joined.column_by_name("item").unwrap().is_null(0));
        }
        ids.push(
            batch["entity_id"]
                .as_primitive::<arrow_array::types::UInt64Type>()
                .value(0),
        );
        count += 1;
    }
    assert_eq!(ids, [2, 8, 2, 20]);
    assert_eq!(count, 4);
    let requests = observations.requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    assert!(
        requests
            .iter()
            .all(|request| request.options == byte_options)
    );
    drop(requests);
    let reports = reports.lock().unwrap();
    assert_eq!(reports.len(), 4);
    assert!(
        reports
            .iter()
            .find(|(key, _)| key == missing)
            .unwrap()
            .1
            .contains(&"item:20".into())
    );
    drop(reports);
    use open_arrow_access_core::AccessorExt;
    use open_arrow_access_layers::ParallelReadLayer;
    let parallel = pages.layer(ParallelReadLayer::new(
        std::num::NonZeroUsize::new(2).unwrap(),
        std::num::NonZeroUsize::MIN,
    ));
    let mut reader = parallel
        .open(
            FileSetRequest::new([item, part])
                .with_options(ReadOptions::default().with_projection(["metadata", "page_id"])),
        )
        .await
        .unwrap();
    let mut count = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.schema().field(0).name(), "metadata");
        assert_eq!(batch.schema().field(1).name(), "page_id");
        assert_eq!(batch.num_columns(), 2);
        count += batch.num_rows();
    }
    assert_eq!(count, 2);
}

#[tokio::test]
async fn duckdb_reader_propagates_report_and_ocr_errors() {
    use open_arrow_access_core::{Error, ErrorKind, ObjectRef};
    use open_arrow_access_test_support::MemoryAccessor;
    use std::sync::Arc;
    let metadata = Arc::new(super::super::metadata::tests::load().await);
    let key = "ocr/item-2/item-2-3-5.txt";
    let bytes = MemoryAccessor::new(b"ocr", 3);
    let observations = bytes.observations.clone();
    let rejecting = BhlPageAccessor::from_metadata(bytes, "inventory", metadata.clone(), |_| {
        Err(Error::new(
            ErrorKind::InvalidInput,
            "inspect_bhl_metadata",
            "rejected",
        ))
    })
    .unwrap();
    let mut reader = rejecting.open(FileSetRequest::new([key])).await.unwrap();
    assert_eq!(
        reader.next().await.unwrap_err().operation(),
        "inspect_bhl_metadata"
    );
    assert!(observations.requests.lock().unwrap().is_empty());
    assert!(reader.next().await.unwrap().is_none());

    let missing = BhlPageAccessor::from_metadata(
        MemoryAccessor::new(b"", 1).with_open_failure(ErrorKind::NotFound),
        "inventory",
        metadata.clone(),
        |_| Ok(()),
    )
    .unwrap();
    let mut reader = missing.open(FileSetRequest::new([key])).await.unwrap();
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::NotFound);

    assert!(
        missing
            .plan_scan(FileSetRequest::new([ObjectRef::new("invalid")]))
            .await
            .is_err()
    );
}

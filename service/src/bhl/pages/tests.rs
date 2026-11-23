use super::*;
use crate::FileSetRequest;
use arrow_array::{Array, RecordBatch, cast::AsArray, types::UInt64Type};
use arrow_schema::{DataType, Fields};
use futures::TryStreamExt;
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteReadOptions, Error, ErrorKind, ReadOptions, RecordReader,
    ScanPlanner, into_stream,
};
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::{MemoryAccessor, temp_fs};
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

const ITEM: &str = "ocr/item-2/item-2-3-5.txt";
const PART: &str = "ocr/part-8/part-8-3-1.txt";
const MISSING: &str = "ocr/item-20/item-20-30-1.txt";

async fn read(
    pages: &impl Accessor<Request = FileSetRequest, Reader = dyn RecordReader>,
    request: FileSetRequest,
) -> Vec<RecordBatch> {
    into_stream(pages.open(request).await.unwrap())
        .try_collect()
        .await
        .unwrap()
}

fn fields(data_type: &DataType) -> &Fields {
    match data_type {
        DataType::Struct(fields) => fields,
        DataType::List(item) => fields(item.data_type()),
        _ => panic!("expected metadata struct/list"),
    }
}

#[tokio::test]
async fn supplied_metadata_preserves_page_bytes_and_identity() {
    let item = "ocr/item-000001/item-000001-00000003-0002.txt";
    let part = "ocr/part-000008/part-000008-00000003-0001.txt";
    let (_root, operator) = temp_fs(&[(item, b"\r\n\xff"), (part, b"")]);
    let pages = BhlPageAccessor::new(OpenDalAccessor::new(operator), "inventory", |_| {
        Ok(super::super::page::tests::metadata("title", "rights"))
    })
    .unwrap();
    let batches = read(&pages, FileSetRequest::new([item, part])).await;
    assert_eq!(batches.len(), 2);
    for (batch, (id, bytes)) in batches
        .iter()
        .zip([(1, b"\r\n\xff".as_slice()), (8, b"".as_slice())])
    {
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch["entity_id"].as_primitive::<UInt64Type>().value(0), id);
        assert_eq!(batch["data"].as_binary::<i64>().value(0), bytes);
        assert!(!batch["metadata"].is_null(0));
        assert!(batch["source_entry_index"].is_null(0));
    }
}

#[test]
fn schema_and_page_identity_match_the_contract() {
    let schema = page_schema();
    let tables = fields(schema.field_with_name("metadata").unwrap().data_type());
    assert_eq!(tables.len(), 13);
    assert_eq!(
        tables
            .iter()
            .map(|table| fields(table.data_type()).len())
            .sum::<usize>(),
        104
    );
    assert_eq!(
        BhlPageIdentity::parse("ocr/item-1/item-1-3-0000.txt")
            .unwrap()
            .sequence,
        0
    );
    for key in [
        "ocr/item-1/part-1-3-1.txt",
        "ocr/item-1/item-2-3-1.txt",
        "ocr/item-1/item-1-0-1.txt",
        "ocr/item-0/item-0-3-1.txt",
        "ocr/item-1/item-1-3-18446744073709551616.txt",
    ] {
        assert!(BhlPageIdentity::parse(key).is_err());
    }
}

#[tokio::test]
async fn duckdb_pages_preserve_order_metadata_reports_and_projection() {
    let metadata = Arc::new(super::super::metadata::tests::load().await);
    let bytes = MemoryAccessor::new(b"\r\n\xff", 2);
    let observations = bytes.observations.clone();
    let reports = Arc::new(Mutex::new(Vec::new()));
    let captured = reports.clone();
    let pages = BhlPageAccessor::from_metadata(bytes, "inventory", metadata.clone(), move |page| {
        captured.lock().unwrap().push(page.missing.clone());
        Ok(())
    })
    .unwrap()
    .with_schema_metadata(std::collections::HashMap::from([(
        "caller".into(),
        "value".into(),
    )]));
    let byte_options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(2).unwrap(),
        io_chunk_size: NonZeroUsize::new(2),
    };
    let request = FileSetRequest::new([ITEM, PART, ITEM, MISSING]).with_byte_options(byte_options);
    let plan = pages.plan_scan(request.clone()).await.unwrap();
    assert!(plan.streaming_discovery);
    assert!(observations.requests.lock().unwrap().is_empty());
    assert!(reports.lock().unwrap().is_empty());
    let schema = plan.metadata.schema;
    assert_eq!(
        schema.metadata()["bhl.metadata.fingerprint"],
        metadata.info.fingerprint
    );
    assert_eq!(schema.metadata()["caller"], "value");
    let batches = read(&pages, request).await;
    assert_eq!(batches.len(), 4);
    for (batch, id) in batches.iter().zip([2, 8, 2, 20]) {
        assert_eq!(batch.schema(), schema);
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch["entity_id"].as_primitive::<UInt64Type>().value(0), id);
        assert_eq!(batch["data"].as_binary::<i64>().value(0), b"\r\n\xff");
        let joined = batch["metadata"].as_struct();
        if id == 20 {
            assert_eq!(
                joined
                    .column_by_name("item")
                    .unwrap()
                    .as_list::<i32>()
                    .value_length(0),
                0
            );
        } else {
            assert_eq!(
                joined
                    .column_by_name("title")
                    .unwrap()
                    .as_list::<i32>()
                    .value(0)
                    .as_struct()
                    .column_by_name("TitleID")
                    .unwrap()
                    .as_string::<i64>()
                    .value(0),
                "001"
            );
        }
    }
    let requests = observations.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 4);
    assert!(
        requests
            .iter()
            .all(|request| request.options == byte_options)
    );
    let reports = reports.lock().unwrap().clone();
    assert_eq!(reports.len(), 4);
    assert_eq!(
        reports[3],
        ["page:30", "item:20", "title:unresolved parent"]
    );

    let parallel = pages.layer(ParallelReadLayer::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::MIN,
    ));
    let batches = read(
        &parallel,
        FileSetRequest::new([ITEM, PART])
            .with_options(ReadOptions::default().with_projection(["metadata", "page_id"])),
    )
    .await;
    assert_eq!(batches.len(), 2);
    for batch in batches {
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(0).name(), "metadata");
        assert_eq!(batch.schema().field(1).name(), "page_id");
    }
    assert_eq!(observations.requests.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn reader_propagates_report_and_ocr_errors() {
    let metadata = Arc::new(super::super::metadata::tests::load().await);
    let bytes = MemoryAccessor::new(b"ocr", 3);
    let observations = bytes.observations.clone();
    let rejecting =
        BhlPageAccessor::from_metadata(bytes.clone(), "inventory", metadata.clone(), |_| {
            Err(Error::new(
                ErrorKind::InvalidInput,
                "inspect_bhl_metadata",
                "rejected",
            ))
        })
        .unwrap();
    let mut reader = rejecting.open(FileSetRequest::new([ITEM])).await.unwrap();
    assert_eq!(
        reader.next().await.unwrap_err().operation(),
        "inspect_bhl_metadata"
    );
    assert!(observations.requests.lock().unwrap().is_empty());
    assert!(reader.next().await.unwrap().is_none());

    let missing = BhlPageAccessor::from_metadata(
        bytes.with_open_failure(ErrorKind::NotFound),
        "inventory",
        metadata,
        |_| Ok(()),
    )
    .unwrap();
    let mut reader = missing.open(FileSetRequest::new([ITEM])).await.unwrap();
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::NotFound);
    assert!(
        missing
            .plan_scan(FileSetRequest::new(["invalid"]))
            .await
            .is_err()
    );
}

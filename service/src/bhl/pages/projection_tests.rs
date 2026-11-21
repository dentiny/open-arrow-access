use super::*;
use crate::FileSetRequest;
use arrow_array::{cast::AsArray, types::UInt64Type};
use open_arrow_access_core::{Accessor, ByteReadOptions, ErrorKind, ReadOptions, ScanPlanner};
use open_arrow_access_test_support::MemoryAccessor;
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

const ITEM: &str = "ocr/item-2/item-2-3-5.txt";
const PART: &str = "ocr/part-8/part-8-3-1.txt";

#[tokio::test]
async fn duckdb_projections_without_data_skip_ocr_and_keep_rows_and_provenance() {
    let metadata = Arc::new(super::super::metadata::tests::load().await);
    let storage = MemoryAccessor::new(b"unread OCR", 3).with_open_failure(ErrorKind::NotFound);
    let observed = storage.observations.clone();
    let pages =
        BhlPageAccessor::from_metadata(storage, "snapshot", metadata.clone(), |_| Ok(())).unwrap();
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
                FileSetRequest::new([ITEM, PART])
                    .with_options(ReadOptions::default().with_projection(columns.clone())),
            )
            .await
            .unwrap();
        for key in [ITEM, PART] {
            let batch = reader.next().await.unwrap().unwrap();
            assert_eq!(batch.num_rows(), 1);
            let schema = batch.schema();
            assert_eq!(
                schema
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<_>>(),
                columns
            );
            assert_eq!(
                schema.metadata()["bhl.metadata.fingerprint"],
                metadata.info.fingerprint
            );
            if let Some(id) = batch.column_by_name("page_id") {
                assert_eq!(id.as_primitive::<UInt64Type>().value(0), 3);
            }
            if let Some(metadata) = batch.column_by_name("metadata") {
                assert_eq!(
                    metadata
                        .as_struct()
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
            if let Some(path) = batch.column_by_name("source_path") {
                assert!(path.as_string::<i64>().value(0).ends_with(key));
            }
        }
        assert!(reader.next().await.unwrap().is_none());
    }
    assert!(observed.requests.lock().unwrap().is_empty());
    assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
    let mut reader = pages
        .open(
            FileSetRequest::new([ITEM])
                .with_options(ReadOptions::default().with_projection(["data"])),
        )
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::NotFound);
    assert_eq!(observed.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn duckdb_data_projection_preserves_bytes_and_io_options() {
    let metadata = Arc::new(super::super::metadata::tests::load().await);
    for bytes in [b"\r\n\xff".as_slice(), b""] {
        let storage = MemoryAccessor::new(bytes, 1);
        let observed = storage.observations.clone();
        let pages =
            BhlPageAccessor::from_metadata(storage, "snapshot", metadata.clone(), |_| Ok(()))
                .unwrap();
        let options = ByteReadOptions {
            io_chunk_size: NonZeroUsize::new(64 * 1024),
            ..Default::default()
        };
        let mut reader = pages
            .open(
                FileSetRequest::new([ITEM])
                    .with_byte_options(options)
                    .with_options(ReadOptions::default().with_projection(["data", "page_id"])),
            )
            .await
            .unwrap();
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.schema().field(0).name(), "data");
        assert_eq!(batch["data"].as_binary::<i64>().value(0), bytes);
        assert!(reader.next().await.unwrap().is_none());
        let requests = observed.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].options, options);
    }
}

#[tokio::test]
async fn invalid_duckdb_projections_fail_before_reports_or_ocr_work() {
    let metadata = Arc::new(super::super::metadata::tests::load().await);
    let storage = MemoryAccessor::new(b"OCR", 3);
    let observed = storage.observations.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let pages = BhlPageAccessor::from_metadata(storage, "snapshot", metadata, move |_| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .unwrap();
    for fields in [vec!["missing"], vec!["data", "data"]] {
        assert!(
            pages
                .plan_scan(
                    FileSetRequest::new([ITEM])
                        .with_options(ReadOptions::default().with_projection(fields),)
                )
                .await
                .is_err()
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(observed.requests.lock().unwrap().is_empty());
}

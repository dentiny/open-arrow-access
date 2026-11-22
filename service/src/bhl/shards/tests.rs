use std::sync::Arc;

use arrow_array::{RecordBatch, cast::AsArray};
use futures::TryStreamExt;
use open_arrow_access_core::{Accessor, ErrorKind, ReadOptions, Result, into_stream};
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::{expect_err_kind, record::field_names, temp_fs};

use super::{BhlShardAccessor, BhlShardReadRequest};
use crate::bhl::metadata::tests::load;

async fn fixture(
    files: &[(&str, &[u8])],
) -> (tempfile::TempDir, BhlShardAccessor<OpenDalAccessor>) {
    let (root, operator) = temp_fs(files);
    let source =
        BhlShardAccessor::from_metadata(OpenDalAccessor::new(operator), Arc::new(load().await))
            .unwrap();
    (root, source)
}

async fn read(
    source: &BhlShardAccessor<OpenDalAccessor>,
    request: BhlShardReadRequest,
) -> Result<Vec<RecordBatch>> {
    into_stream(source.open(request).await?).try_collect().await
}

#[test]
fn directory_batches_validate_and_preserve_selection() {
    let request = BhlShardReadRequest::from_prefixes(["ocr/item-0002/", "ocr/part-8/"]).unwrap();
    assert_eq!(request.prefix(), "ocr/item-0002/");
    assert_eq!(request.prefixes(), ["ocr/item-0002/", "ocr/part-8/"]);
    assert_eq!(
        BhlShardReadRequest::new("ocr/item-2/").unwrap().prefixes(),
        ["ocr/item-2/"]
    );
    assert!(BhlShardReadRequest::from_prefixes(Vec::<String>::new()).is_err());
    for prefixes in [
        vec!["ocr/item-2/", "ocr/item-2/"],
        vec!["ocr/item-2/", "ocr/"],
        vec!["ocr/item-2/", "ocr/part-0/"],
        vec!["ocr/item-2/", "ocr/part-8/subdirectory/"],
    ] {
        assert!(BhlShardReadRequest::from_prefixes(prefixes).is_err());
    }
}

#[tokio::test]
async fn directory_batch_shares_schema_metadata_projection_and_original_bytes() {
    let (root, source) = fixture(&[
        ("ocr/item-0002/item-0002-3-5.txt", b"\r\n\xff"),
        ("ocr/item-0002/item-0002.txt", b"aggregate"),
        ("ocr/part-8/part-8-3-1.txt", b""),
        ("ocr/part-8/part-8.txt", b"aggregate"),
    ])
    .await;
    let source = source.with_snapshot_id("batch").unwrap();
    let request = BhlShardReadRequest::from_prefixes(["ocr/item-0002/", "ocr/part-8/"]).unwrap();
    let batches = read(&source, request.clone()).await.unwrap();
    assert_eq!(batches.len(), 2);
    let expected = [b"\r\n\xff".as_slice(), b"".as_slice()];
    for (batch, bytes) in batches.iter().zip(expected) {
        assert_eq!(batch["data"].as_binary::<i64>().value(0), bytes);
        assert_eq!(batch["snapshot_id"].as_string::<i32>().value(0), "batch");
        assert_eq!(batch.schema(), batches[0].schema());
    }
    let request =
        request.with_options(ReadOptions::default().with_projection(["source_path", "metadata"]));
    let mut reader = source.open(request.clone()).await.unwrap();
    tokio::fs::remove_dir_all(root.path().join("ocr"))
        .await
        .unwrap();
    let schema = source.metadata(request).await.unwrap().schema;
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.schema(), schema);
        assert_eq!(field_names(&schema), ["source_path", "metadata"]);
        rows += batch.num_rows();
    }
    assert_eq!(rows, 2);
}

#[tokio::test]
async fn directory_batch_rejects_each_empty_or_malformed_directory() {
    let (_root, source) = fixture(&[
        ("ocr/item-2/item-2-3-5.txt", b"page"),
        ("ocr/part-8/part-8.txt", b"aggregate"),
        ("ocr/item-99/unexpected.txt", b"invalid"),
    ])
    .await;
    for prefix in ["ocr/part-8/", "ocr/item-99/", "ocr/item-20/"] {
        let request = BhlShardReadRequest::from_prefixes(["ocr/item-2/", prefix]).unwrap();
        source.metadata(request.clone()).await.unwrap();
        let error = expect_err_kind(source.open(request).await, ErrorKind::InvalidInput);
        assert!(error.to_string().contains(prefix), "{error}");
    }
}

#[tokio::test]
async fn directory_batch_keeps_strict_metadata_reports() {
    let missing = "ocr/item-20/item-20-30-1.txt";
    let (_root, source) = fixture(&[
        ("ocr/item-2/item-2-3-5.txt", b"valid"),
        (missing, b"missing"),
    ])
    .await;
    let request = BhlShardReadRequest::from_prefixes(["ocr/item-2/", "ocr/item-20/"]).unwrap();
    let error = expect_err_kind(read(&source, request).await, ErrorKind::InvalidInput);
    assert_eq!(error.object().unwrap().locator(), missing);
}

#[tokio::test]
async fn reads_five_hundred_directories_through_one_native_scan() {
    use arrow_array::types::UInt64Type;
    use std::{
        collections::HashSet,
        sync::atomic::{AtomicUsize, Ordering},
    };

    let metadata = Arc::new(crate::bhl::metadata::tests::load_with_sql(
        "INSERT INTO item
         SELECT * EXCLUDE (id) REPLACE (CAST(id AS VARCHAR) AS ItemID)
         FROM item CROSS JOIN range(1000, 1500) AS ids(id) WHERE ItemID = '2';
         INSERT INTO page
         SELECT * EXCLUDE (id) REPLACE (CAST(id * 10 AS VARCHAR) AS PageID, CAST(id AS VARCHAR) AS ItemID)
         FROM page CROSS JOIN range(1000, 1500) AS ids(id) WHERE PageID = '3';"
    ).await);
    let root = tempfile::tempdir().unwrap();
    let mut prefixes = Vec::new();
    let payload = b"batch OCR\r\n\xff";
    for id in 1000..1500 {
        let prefix = format!("ocr/item-{id}/");
        let directory = root.path().join(&prefix);
        tokio::fs::create_dir_all(&directory).await.unwrap();
        tokio::fs::write(
            directory.join(format!("item-{id}-{}-5.txt", id * 10)),
            payload,
        )
        .await
        .unwrap();
        tokio::fs::write(directory.join(format!("item-{id}.txt")), b"aggregate")
            .await
            .unwrap();
        prefixes.push(prefix);
    }
    let reports = Arc::new(AtomicUsize::new(0));
    let reported = reports.clone();
    let source = BhlShardAccessor::from_metadata(
        OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(root.path())),
        metadata,
    )
    .unwrap()
    .with_snapshot_id("batch-500")
    .unwrap()
    .with_report(move |page| {
        assert!(page.missing.is_empty() && page.conflicts.is_empty());
        reported.fetch_add(1, Ordering::SeqCst);
        Ok(())
    });
    let request = BhlShardReadRequest::from_prefixes(prefixes).unwrap();
    assert_eq!(request.prefixes().len(), 500);
    let mut reader = source.open(request).await.unwrap();
    let mut ids = HashSet::new();
    while let Some(batch) = reader.next().await.unwrap() {
        let id = batch["entity_id"].as_primitive::<UInt64Type>().value(0);
        assert!((1000..1500).contains(&id));
        assert!(ids.insert(id));
        assert_eq!(
            batch["page_id"].as_primitive::<UInt64Type>().value(0),
            id * 10
        );
        assert_eq!(batch["data"].as_binary::<i64>().value(0), payload);
        assert_eq!(
            batch["snapshot_id"].as_string::<i32>().value(0),
            "batch-500"
        );
        assert_eq!(batch.num_rows(), 1);
    }
    assert_eq!(ids.len(), 500);
    assert_eq!(reports.load(Ordering::SeqCst), 500);
}

#[tokio::test]
async fn reads_one_shard_and_projects_metadata_without_ocr() {
    let key = "ocr/item-0002/item-0002-0003-0005.txt";
    let (root, source) = fixture(&[
        (key, b"\r\n\xff"),
        ("ocr/item-0002/item-0002.txt", b"aggregate"),
        ("ocr/part-8/part-8-3-1.txt", b"other shard"),
    ])
    .await;
    let source = source.with_snapshot_id("inventory").unwrap();
    let request = BhlShardReadRequest::new("ocr/item-0002/").unwrap();
    let batches = read(&source, request.clone()).await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0]["data"].as_binary::<i64>().value(0), b"\r\n\xff");
    assert_eq!(
        batches[0]["snapshot_id"].as_string::<i32>().value(0),
        "inventory"
    );

    let request =
        request.with_options(ReadOptions::default().with_projection(["page_id", "metadata"]));
    let mut reader = source.open(request.clone()).await.unwrap();
    std::fs::remove_file(root.path().join(key)).unwrap();
    // Schema inspection needs no listing; projected reads need no OCR body.
    let metadata = source.metadata(request).await.unwrap();
    assert_eq!(reader.metadata().schema, metadata.schema);
    assert!(
        metadata
            .schema
            .metadata()
            .contains_key("bhl.metadata.fingerprint")
    );
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(
        field_names(batch.schema().as_ref()),
        ["page_id", "metadata"]
    );
    assert_eq!(batch.num_rows(), 1);
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn rejects_invalid_or_empty_shards() {
    for prefix in ["ocr/", "ocr/item-2"] {
        assert!(BhlShardReadRequest::new(prefix).is_err());
    }
    let (_root, source) = fixture(&[
        ("ocr/item-2/item-2.txt", b"aggregate"),
        ("ocr/part-8/unexpected.txt", b"invalid"),
    ])
    .await;
    for prefix in ["ocr/item-2/", "ocr/part-8/", "ocr/item-99/"] {
        expect_err_kind(
            source.open(BhlShardReadRequest::new(prefix).unwrap()).await,
            ErrorKind::InvalidInput,
        );
    }
}

#[tokio::test]
async fn metadata_reports_are_strict_unless_overridden() {
    let missing = "ocr/item-20/item-20-30-1.txt";
    let conflict = "ocr/item-2/item-2-3-6.txt";
    let (_root, source) = fixture(&[(missing, b"missing"), (conflict, b"conflict")]).await;
    for (prefix, key) in [("ocr/item-20/", missing), ("ocr/item-2/", conflict)] {
        let request = BhlShardReadRequest::new(prefix).unwrap();
        let error = expect_err_kind(
            read(&source, request.clone()).await,
            ErrorKind::InvalidInput,
        );
        assert_eq!(error.object().unwrap().locator(), key);
        let permissive = source.clone().with_report(|page| {
            assert!(!page.missing.is_empty() || !page.conflicts.is_empty());
            Ok(())
        });
        let batches = read(&permissive, request).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }
}

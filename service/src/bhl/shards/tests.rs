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

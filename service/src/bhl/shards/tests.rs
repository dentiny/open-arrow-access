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
async fn reads_five_hundred_directories_through_one_native_scan() {
    use arrow_array::types::UInt64Type;
    use std::collections::HashSet;

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
        tokio::fs::write(
            directory.join(format!("item-{id}-{}-4.txt", id * 10)),
            b"different alternative OCR",
        )
        .await
        .unwrap();
        tokio::fs::write(directory.join(format!("item-{id}.txt")), b"aggregate")
            .await
            .unwrap();
        prefixes.push(prefix);
    }
    let source = BhlShardAccessor::from_metadata(
        OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(root.path())),
        metadata,
    )
    .unwrap();
    let alternatives = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let captured = alternatives.clone();
    let source = source.with_alternative_report(move |_| {
        captured.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    });
    let batches = read(
        &source,
        BhlShardReadRequest::from_prefixes(prefixes).unwrap(),
    )
    .await
    .unwrap();
    let mut ids = HashSet::new();
    for batch in batches {
        let id = batch["entity_id"].as_primitive::<UInt64Type>().value(0);
        assert!((1000..1500).contains(&id) && ids.insert(id));
        assert_eq!(batch["data"].as_binary::<i64>().value(0), payload);
    }
    assert_eq!(ids.len(), 500);
    assert_eq!(alternatives.load(std::sync::atomic::Ordering::Relaxed), 500);
}

#[tokio::test]
async fn reads_single_or_batched_shards_and_projects_without_ocr() {
    for batched in [false, true] {
        let (root, source) = fixture(&[
            ("ocr/item-0002/item-0002-3-5.txt", b"\r\n\xff"),
            ("ocr/item-0002/item-0002.txt", b"aggregate"),
            ("ocr/part-8/part-8-3-1.txt", b""),
            ("ocr/part-8/part-8.txt", b"aggregate"),
        ])
        .await;
        let source = source.with_snapshot_id("inventory").unwrap();
        let request = if batched {
            BhlShardReadRequest::from_prefixes(["ocr/item-0002/", "ocr/part-8/"])
        } else {
            BhlShardReadRequest::new("ocr/item-0002/")
        }
        .unwrap();
        let expected_rows = request.prefixes().len();
        let batches = read(&source, request.clone()).await.unwrap();
        let expected = [b"\r\n\xff".as_slice(), b"".as_slice()];
        assert_eq!(batches.len(), expected_rows);
        for (batch, bytes) in batches.iter().zip(expected) {
            assert_eq!(batch["data"].as_binary::<i64>().value(0), bytes);
            assert_eq!(
                batch["snapshot_id"].as_string::<i32>().value(0),
                "inventory"
            );
        }
        let request =
            request.with_options(ReadOptions::default().with_projection(["page_id", "metadata"]));
        let mut reader = source.open(request.clone()).await.unwrap();
        tokio::fs::remove_dir_all(root.path().join("ocr"))
            .await
            .unwrap();
        let schema = source.metadata(request).await.unwrap().schema;
        assert!(schema.metadata().contains_key("bhl.metadata.fingerprint"));
        let mut rows = 0;
        while let Some(batch) = reader.next().await.unwrap() {
            assert_eq!(batch.schema(), schema);
            assert_eq!(field_names(&schema), ["page_id", "metadata"]);
            rows += batch.num_rows();
        }
        assert_eq!(rows, expected_rows);
    }
}

#[tokio::test]
async fn rejects_invalid_or_empty_shards() {
    for prefix in ["ocr/", "ocr/item-2"] {
        assert!(BhlShardReadRequest::new(prefix).is_err());
    }
    let (_root, source) = fixture(&[
        ("ocr/item-2/item-2-3-5.txt", b"valid"),
        ("ocr/part-8/part-8.txt", b"aggregate"),
        ("ocr/item-99/unexpected.txt", b"invalid"),
    ])
    .await;
    for prefix in ["ocr/part-8/", "ocr/item-99/", "ocr/item-20/"] {
        for request in [
            BhlShardReadRequest::new(prefix).unwrap(),
            BhlShardReadRequest::from_prefixes(["ocr/item-2/", prefix]).unwrap(),
        ] {
            source.metadata(request.clone()).await.unwrap();
            let error = expect_err_kind(source.open(request).await, ErrorKind::InvalidInput);
            assert!(error.to_string().contains(prefix), "{error}");
        }
    }
}

#[tokio::test]
async fn parent_conflicts_remain_strict_unless_overridden() {
    let key = "ocr/item-99/item-99-3-5.txt";
    let (_root, source) = fixture(&[(key, b"conflicting parent")]).await;
    let request = BhlShardReadRequest::new("ocr/item-99/").unwrap();
    let error = expect_err_kind(
        read(&source, request.clone()).await,
        ErrorKind::InvalidInput,
    );
    assert_eq!(error.object().unwrap().locator(), key);
    let permissive = source.with_report(|page| {
        assert!(!page.conflicts.is_empty());
        Ok(())
    });
    assert_eq!(read(&permissive, request).await.unwrap().len(), 1);
}

#[tokio::test]
async fn selects_item_and_part_versions_and_audits_without_reading_alternatives() {
    use arrow_array::types::UInt64Type;
    use std::sync::Mutex;

    let (root, source) = fixture(&[
        ("ocr/item-2/item-2-3-4.txt", b"different stale body"),
        ("ocr/item-2/item-2-3-5.txt", b"canonical item"),
        ("ocr/part-8/part-8-3-0.txt", b"different part body"),
        ("ocr/part-8/part-8-3-1.txt", b"canonical part"),
    ])
    .await;
    let audit = Arc::new(Mutex::new(Vec::new()));
    let captured = audit.clone();
    let source = source.with_alternative_report(move |version| {
        captured.lock().unwrap().push(version.clone());
        Ok(())
    });
    let request = BhlShardReadRequest::from_prefixes(["ocr/item-2/", "ocr/part-8/"]).unwrap();
    let mut reader = source.open(request).await.unwrap();
    for version in audit.lock().unwrap().iter() {
        std::fs::remove_file(root.path().join(version.alternative.locator())).unwrap();
    }
    let mut batches = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        batches.push(batch);
    }
    assert_eq!(batches.len(), 2);
    for (batch, (sequence, body)) in batches.iter().zip([
        (5, b"canonical item".as_slice()),
        (1, b"canonical part".as_slice()),
    ]) {
        assert_eq!(
            batch["sequence"].as_primitive::<UInt64Type>().value(0),
            sequence
        );
        assert_eq!(batch["data"].as_binary::<i64>().value(0), body);
    }
    let audit = audit.lock().unwrap();
    assert_eq!(audit.len(), 2);
    assert_eq!(audit[0].alternative.locator(), "ocr/item-2/item-2-3-4.txt");
    assert_eq!(audit[0].canonical.locator(), "ocr/item-2/item-2-3-5.txt");
    assert_eq!(audit[1].sequence, 1);
}

#[tokio::test]
async fn rejects_missing_or_ambiguous_canonical_keys_even_with_permissive_metadata_policy() {
    for files in [
        vec![("ocr/item-2/item-2-3-4.txt", b"stale".as_slice())],
        vec![
            ("ocr/item-2/item-2-3-5.txt", b"first".as_slice()),
            ("ocr/item-2/item-2-3-0005.txt", b"second".as_slice()),
        ],
        vec![("ocr/item-2/item-2-30-1.txt", b"no metadata".as_slice())],
    ] {
        let (_root, source) = fixture(&files).await;
        let source = source.with_report(|_| Ok(()));
        let request = BhlShardReadRequest::new("ocr/item-2/").unwrap();
        let error = expect_err_kind(source.open(request).await, ErrorKind::InvalidInput);
        assert!(error.to_string().contains("SequenceOrder"), "{error}");
        assert!(error.to_string().contains("ocr/item-2/"), "{error}");
    }
}

#[tokio::test]
async fn rejects_ambiguous_or_invalid_authoritative_sequences() {
    use open_arrow_access_core::ObjectRef;
    for sql in [
        "INSERT INTO page SELECT * FROM page WHERE PageID = '3'",
        "INSERT INTO page SELECT * REPLACE ('6' AS SequenceOrder) FROM page WHERE PageID = '3'",
        "UPDATE page SET SequenceOrder = '' WHERE PageID = '3'",
        "UPDATE page SET SequenceOrder = '0' WHERE PageID = '3'",
        "INSERT INTO partpage SELECT * FROM partpage WHERE PartID = '8'",
    ] {
        let metadata = crate::bhl::metadata::tests::load_with_sql(sql).await;
        let key = if sql.contains("partpage") {
            "ocr/part-8/part-8-3-1.txt"
        } else {
            "ocr/item-2/item-2-3-5.txt"
        };
        let error = expect_err_kind(
            metadata.canonical_keys(vec![ObjectRef::new(key)], |_| Ok(())),
            ErrorKind::InvalidInput,
        );
        assert!(error.to_string().contains("SequenceOrder"), "{error}");
        assert_eq!(error.object().unwrap().locator(), key);
    }
}

#[tokio::test]
async fn audit_errors_stop_planning_before_ocr_reads() {
    let (_root, source) = fixture(&[
        ("ocr/item-2/item-2-3-4.txt", b"stale"),
        ("ocr/item-2/item-2-3-5.txt", b"canonical"),
    ])
    .await;
    let source = source
        .with_alternative_report(|_| Err(crate::bhl::error::invalid_pages("audit unavailable")));
    let error = expect_err_kind(
        source
            .open(BhlShardReadRequest::new("ocr/item-2/").unwrap())
            .await,
        ErrorKind::InvalidInput,
    );
    assert!(error.to_string().contains("audit unavailable"));
}

#[tokio::test]
async fn reconciles_real_keys_from_all_five_failed_batches() {
    use open_arrow_access_core::ObjectRef;
    // Official bucket listings and an independent metadata snapshot, 2026-11-22.
    // Item 51616 also has positive off-by-one versions with different bodies.
    let examples = [
        (7219, 760784, 1),
        (14432, 551603, 1),
        (15702, 875958, 1),
        (51616, 14909012, 1),
        (108745, 34979650, 1),
        (51616, 14908961, 20),
    ];
    let mut sql = String::new();
    let mut keys = Vec::new();
    let mut expected = Vec::new();
    for (item, page, sequence) in examples {
        sql.push_str(&format!(
            "INSERT INTO page SELECT * REPLACE ('{item}' AS ItemID, '{page}' AS PageID, '{sequence}' AS SequenceOrder) FROM page WHERE PageID = '3';"
        ));
        let key = |seq| {
            ObjectRef::new(format!(
                "ocr/item-{item:06}/item-{item:06}-{page:08}-{seq:04}.txt"
            ))
        };
        keys.extend([key(sequence - 1), key(sequence)]);
        expected.push(key(sequence));
    }
    let metadata = crate::bhl::metadata::tests::load_with_sql(&sql).await;
    let mut audit = Vec::new();
    let selected = metadata
        .canonical_keys(keys, |version| {
            audit.push(version.alternative.clone());
            Ok(())
        })
        .unwrap();
    assert_eq!(
        selected.iter().map(|key| key.locator()).collect::<Vec<_>>(),
        expected.iter().map(|key| key.locator()).collect::<Vec<_>>()
    );
    assert_eq!(audit.len(), examples.len());
}

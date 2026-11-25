use super::*;
use crate::{FileSetRequest, bhl::metadata::tests::load};
use arrow_array::cast::AsArray;
use futures::TryStreamExt;
use open_arrow_access_core::{Accessor, ErrorKind, ReadOptions, into_stream};
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::temp_fs;
use std::sync::{Arc, Mutex};

const CANONICAL: &str = "ocr/item-2/item-2-3-5.txt";
const ALIAS: &str = "ocr/item-2/item-2-0003-0005.txt";
const ORPHAN: &str = "ocr/item-2/item-2-30-1.txt";

async fn source(files: &[(&str, &[u8])]) -> (tempfile::TempDir, BhlPageAccessor<OpenDalAccessor>) {
    let (root, storage) = temp_fs(files);
    let pages = BhlPageAccessor::from_metadata(
        OpenDalAccessor::new(storage),
        "snapshot",
        Arc::new(load().await),
        crate::bhl::error::require_page_metadata,
    )
    .unwrap();
    (root, pages)
}

#[tokio::test]
async fn identical_canonical_aliases_emit_one_original_payload() {
    for payload in [b"\r\n\xff".as_slice(), b""] {
        let (_root, pages) = source(&[(CANONICAL, payload), (ALIAS, payload)]).await;
        let batches = into_stream(
            pages
                .open(FileSetRequest::new([CANONICAL, ALIAS]))
                .await
                .unwrap(),
        )
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0]["data"].as_binary::<i64>().value(0), payload);
        assert!(
            batches[0]["source_path"]
                .as_string::<i64>()
                .value(0)
                .ends_with(CANONICAL)
        );
    }
}

#[tokio::test]
async fn missing_page_metadata_is_quarantined_before_any_ocr_read() {
    // The orphan is deliberately absent from storage: it must never be opened.
    let (_root, pages) = source(&[(CANONICAL, b"valid")]).await;
    let events = Arc::new(Mutex::new(Vec::new()));
    let sink = events.clone();
    let pages = pages.with_reconciliation_report(move |event| {
        sink.lock().unwrap().push(event.clone());
        Ok(())
    });
    let batches = into_stream(
        pages
            .open(FileSetRequest::new([ORPHAN, CANONICAL]))
            .await
            .unwrap(),
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0]["data"].as_binary::<i64>().value(0), b"valid");
    let events = events.lock().unwrap();
    assert_eq!(events.len(), 1);
    let BhlPageReconciliation::Quarantined {
        object,
        reason,
        missing,
        ..
    } = &events[0]
    else {
        panic!("expected quarantine: {:?}", events[0]);
    };
    assert_eq!(object.locator(), ORPHAN);
    assert!(reason.contains("SequenceOrder"));
    assert!(missing.iter().any(|value| value.contains("page")));
}

#[tokio::test]
async fn verified_deduplication_is_reported_and_sink_errors_stop_output() {
    for fail in [false, true] {
        let (_root, pages) = source(&[(CANONICAL, b"same"), (ALIAS, b"same")]).await;
        let events = Arc::new(Mutex::new(Vec::new()));
        let sink = events.clone();
        let pages = pages.with_reconciliation_report(move |event| {
            sink.lock().unwrap().push(event.clone());
            if fail {
                Err(crate::bhl::error::invalid_pages("audit unavailable"))
            } else {
                Ok(())
            }
        });
        let mut reader = pages
            .open(FileSetRequest::new([CANONICAL, ALIAS]))
            .await
            .unwrap();
        if fail {
            assert!(
                reader
                    .next()
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("audit unavailable")
            );
        } else {
            assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
        }
        assert!(reader.next().await.unwrap().is_none());
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 1);
        let BhlPageReconciliation::Deduplicated {
            canonical,
            duplicate,
        } = &events[0]
        else {
            panic!("expected deduplication: {:?}", events[0]);
        };
        assert_eq!(canonical.locator(), CANONICAL);
        assert_eq!(duplicate.locator(), ALIAS);
    }
}

#[tokio::test]
async fn quarantine_sink_errors_are_not_swallowed() {
    let (_root, pages) = source(&[(CANONICAL, b"valid")]).await;
    let pages = pages
        .with_reconciliation_report(|_| Err(crate::bhl::error::invalid_pages("audit unavailable")));
    let mut reader = pages
        .open(FileSetRequest::new([ORPHAN, CANONICAL]))
        .await
        .unwrap();
    assert!(
        reader
            .next()
            .await
            .unwrap_err()
            .to_string()
            .contains("audit unavailable")
    );
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn different_canonical_payloads_fail_even_without_a_data_projection() {
    for columns in [vec!["data"], vec!["page_id"], vec![]] {
        let (_root, pages) = source(&[(CANONICAL, b"one"), (ALIAS, b"two")]).await;
        let mut reader = pages
            .open(
                FileSetRequest::new([CANONICAL, ALIAS])
                    .with_options(ReadOptions::default().with_projection(columns)),
            )
            .await
            .unwrap();
        let error = reader.next().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(
            error.to_string().contains("different OCR payloads"),
            "{error}"
        );
        assert!(error.to_string().contains(CANONICAL), "{error}");
        assert!(error.to_string().contains(ALIAS), "{error}");
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn an_all_quarantined_selection_still_fails() {
    let (_root, pages) = source(&[(CANONICAL, b"unused")]).await;
    let mut reader = pages.open(FileSetRequest::new([ORPHAN])).await.unwrap();
    let error = reader.next().await.unwrap_err();
    assert!(error.to_string().contains("quarantined"), "{error}");
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn a_missing_duplicate_payload_is_not_silently_deduplicated() {
    let (_root, pages) = source(&[(CANONICAL, b"valid")]).await;
    let mut reader = pages
        .open(FileSetRequest::new([CANONICAL, ALIAS]))
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::NotFound);
}

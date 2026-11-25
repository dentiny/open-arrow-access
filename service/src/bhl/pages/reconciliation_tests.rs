use super::*;
use crate::{FileSetRequest, bhl::metadata::tests::load};
use arrow_array::{RecordBatch, cast::AsArray};
use futures::TryStreamExt;
use open_arrow_access_core::{Accessor, ErrorKind, ReadOptions, into_stream};
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::temp_fs;
use rstest::rstest;
use std::sync::{Arc, Mutex};

const CANONICAL: &str = "ocr/item-2/item-2-3-5.txt";
const ALIAS: &str = "ocr/item-2/item-2-0003-0005.txt";
const ORPHAN: &str = "ocr/item-2/item-2-30-1.txt";

#[rstest]
#[case(
    "ocr/item-4/item-4-3-5.txt",
    "ocr/item-4/item-4-0003-0005.txt",
    "INSERT INTO item SELECT * REPLACE ('4' AS ItemID) FROM item WHERE ItemID='2'",
    "page.ItemID"
)]
#[case(
    "ocr/part-8/part-8-3-1.txt",
    "ocr/part-8/part-8-0003-0001.txt",
    "UPDATE part SET ItemID='9' WHERE PartID='8'",
    "page.ItemID"
)]
#[case(
    "ocr/part-8/part-8-3-1.txt",
    "ocr/part-8/part-8-0003-0001.txt",
    "UPDATE partpage SET ItemID='9' WHERE PartID='8'",
    "partpage.ItemID"
)]
#[tokio::test]
async fn conflicting_parent_groups_are_audited_without_reading_their_ocr(
    #[case] invalid: &str,
    #[case] alias: &str,
    #[case] sql: &str,
    #[case] expected_conflict: &str,
) {
    use crate::bhl::metadata::tests::load_with_sql;
    // Invalid objects are absent from storage. Reading either alias would fail.
    let (_root, storage) = temp_fs(&[(CANONICAL, b"healthy".as_slice())]);
    let metadata = Arc::new(load_with_sql(sql).await);
    let events = Arc::new(Mutex::new(Vec::new()));
    let sink = events.clone();
    let pages = BhlPageAccessor::from_metadata(
        OpenDalAccessor::new(storage),
        "snapshot",
        metadata,
        crate::bhl::error::require_page_metadata,
    )
    .unwrap()
    .with_reconciliation_report(move |event| {
        sink.lock().unwrap().push(event.clone());
        Ok(())
    });
    let batches = into_stream(
        pages
            .open(FileSetRequest::new([invalid, alias, CANONICAL]))
            .await
            .unwrap(),
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0]["data"].as_binary::<i64>().value(0), b"healthy");
    let events = events.lock().unwrap();
    assert_eq!(events.len(), 1);
    let BhlPageReconciliation::ConflictingMetadata {
        canonical,
        aliases,
        conflicts,
        ..
    } = &events[0]
    else {
        panic!("expected metadata quarantine: {:?}", events[0]);
    };
    assert_eq!(canonical.locator(), invalid);
    assert_eq!(
        aliases.iter().map(|o| o.locator()).collect::<Vec<_>>(),
        [invalid, alias]
    );
    assert!(
        conflicts
            .iter()
            .any(|message| message.contains(expected_conflict))
    );
}

#[rstest]
#[case(false, false)]
#[case(true, false)]
#[case(false, true)]
#[case(true, true)]
#[tokio::test]
async fn metadata_quarantine_never_swallows_audit_failure_or_all_empty_output(
    #[case] healthy: bool,
    #[case] fail_sink: bool,
) {
    use crate::bhl::metadata::tests::load_with_sql;
    use open_arrow_access_core::AccessorExt;
    use open_arrow_access_layers::ParallelReadLayer;
    use std::num::NonZeroUsize;
    const INVALID: &str = "ocr/item-4/item-4-3-5.txt";
    let (_root, storage) = temp_fs(&[(CANONICAL, b"healthy".as_slice())]);
    let metadata = Arc::new(
        load_with_sql(
            "INSERT INTO item SELECT * REPLACE ('4' AS ItemID) FROM item WHERE ItemID='2'",
        )
        .await,
    );
    let pages = BhlPageAccessor::from_metadata(
        OpenDalAccessor::new(storage),
        "snapshot",
        metadata,
        crate::bhl::error::require_page_metadata,
    )
    .unwrap()
    .with_reconciliation_report(move |_| {
        if fail_sink {
            Err(crate::bhl::error::invalid_pages("audit unavailable"))
        } else {
            Ok(())
        }
    })
    .layer(ParallelReadLayer::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::MIN,
    ));
    let mut keys = vec![INVALID];
    if healthy {
        keys.push(CANONICAL);
    }
    let result = into_stream(pages.open(FileSetRequest::new(keys)).await.unwrap())
        .try_collect::<Vec<_>>()
        .await;
    if fail_sink {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("audit unavailable")
        );
    } else if healthy {
        assert_eq!(
            result
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            1
        );
    } else {
        assert!(result.unwrap_err().to_string().contains("quarantined"));
    }
}

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
async fn missing_required_parent_metadata_still_reaches_strict_validation() {
    use crate::bhl::metadata::tests::load_with_sql;
    let (_root, storage) = temp_fs(&[]);
    let metadata = Arc::new(load_with_sql("DELETE FROM item").await);
    let pages = BhlPageAccessor::from_metadata(
        OpenDalAccessor::new(storage),
        "snapshot",
        metadata,
        crate::bhl::error::require_page_metadata,
    )
    .unwrap()
    .with_reconciliation_report(|event| panic!("unexpected quarantine: {event:?}"));
    let error = into_stream(pages.open(FileSetRequest::new([CANONICAL])).await.unwrap())
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("missing metadata"), "{error}");
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

#[rstest]
#[case(vec!["data"])]
#[case(vec!["page_id"])]
#[case(vec![])]
#[tokio::test]
async fn conflicting_group_is_quarantined_while_healthy_pages_continue(#[case] columns: Vec<&str>) {
    const HEALTHY: &str = "ocr/part-8/part-8-3-1.txt";
    const THIRD: &str = "ocr/item-2/item-2-03-05.txt";
    let (_root, pages) = source(&[
        (CANONICAL, b"one"),
        (THIRD, b"one"),
        (ALIAS, b"two"),
        (HEALTHY, b"healthy"),
    ])
    .await;
    let events = Arc::new(Mutex::new(Vec::new()));
    let sink = events.clone();
    let pages = pages.with_reconciliation_report(move |event| {
        sink.lock().unwrap().push(event.clone());
        Ok(())
    });
    let batches = into_stream(
        pages
            .open(
                FileSetRequest::new([CANONICAL, THIRD, ALIAS, HEALTHY])
                    .with_options(ReadOptions::default().with_projection(columns)),
            )
            .await
            .unwrap(),
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    if let Some(data) = batches[0].column_by_name("data") {
        assert_eq!(data.as_binary::<i64>().value(0), b"healthy");
    }
    let events = events.lock().unwrap();
    // No earlier alias can be reported as deduplicated once the group conflicts.
    assert_eq!(events.len(), 1);
    let BhlPageReconciliation::ConflictingPayloads {
        canonical,
        conflicting,
        aliases,
    } = &events[0]
    else {
        panic!("expected content quarantine: {:?}", events[0]);
    };
    assert_eq!(canonical.locator(), CANONICAL);
    assert_eq!(conflicting.locator(), ALIAS);
    assert_eq!(
        aliases.iter().map(|v| v.locator()).collect::<Vec<_>>(),
        [CANONICAL, THIRD, ALIAS]
    );
}

#[rstest]
#[case(b"line\n\xff\n".as_slice(), b"line\r\n\xff\r\n".as_slice())]
#[case(b"mixed\r\nline\n".as_slice(), b"mixed\nline\r\n".as_slice())]
#[tokio::test]
async fn line_ending_aliases_preserve_canonical_bytes_and_report_equivalence(
    #[case] canonical: &[u8],
    #[case] alias: &[u8],
) {
    let (_root, pages) = source(&[(CANONICAL, canonical), (ALIAS, alias)]).await;
    let events = Arc::new(Mutex::new(Vec::new()));
    let sink = events.clone();
    let pages = pages.with_reconciliation_report(move |event| {
        sink.lock().unwrap().push(event.clone());
        Ok(())
    });
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
    assert_eq!(batches[0]["data"].as_binary::<i64>().value(0), canonical);
    let events = events.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert!(
        matches!(&events[0], BhlPageReconciliation::LineEndingDeduplicated { canonical, duplicate }
        if canonical.locator() == CANONICAL && duplicate.locator() == ALIAS)
    );
}

#[rstest]
#[case(b"one".as_slice(), b"two".as_slice())]
#[case(b"one\r".as_slice(), b"one".as_slice())]
#[case(b"one \n".as_slice(), b"one\n".as_slice())]
#[case(b"one\r\r\n".as_slice(), b"one\n".as_slice())]
#[tokio::test]
async fn all_content_conflicts_still_fail(#[case] canonical: &[u8], #[case] alias: &[u8]) {
    let (_root, pages) = source(&[(CANONICAL, canonical), (ALIAS, alias)]).await;
    let mut reader = pages
        .open(FileSetRequest::new([CANONICAL, ALIAS]))
        .await
        .unwrap();
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("quarantined"), "{error}");
    assert!(reader.next().await.unwrap().is_none());
}

#[rstest]
#[case(b"same\n".as_slice(), b"same\r\n".as_slice())]
#[case(b"one".as_slice(), b"two".as_slice())]
#[tokio::test]
async fn payload_reconciliation_sink_errors_stop_the_scan(
    #[case] canonical: &[u8],
    #[case] alias: &[u8],
) {
    let (_root, pages) = source(&[(CANONICAL, canonical), (ALIAS, alias)]).await;
    let pages = pages
        .with_reconciliation_report(|_| Err(crate::bhl::error::invalid_pages("audit unavailable")));
    let mut reader = pages
        .open(FileSetRequest::new([CANONICAL, ALIAS]))
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
}

#[rstest]
#[case(false, false)]
#[case(true, false)]
#[case(false, true)]
#[case(true, true)]
fn completion_checks_both_discovery_orders(#[case] discovery_first: bool, #[case] healthy: bool) {
    let completion = super::reconciliation::ScanCompletion::default();
    completion.selected(&open_arrow_access_core::ObjectRef::new(CANONICAL));
    completion.selected(&open_arrow_access_core::ObjectRef::new(ALIAS));
    if discovery_first {
        completion.discovery_finished().unwrap();
    }
    completion.completed(false).unwrap();
    let completed = completion.completed(healthy);
    if discovery_first {
        assert_eq!(completed.is_ok(), healthy);
    } else {
        completed.unwrap();
        assert_eq!(completion.discovery_finished().is_ok(), healthy);
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

#[tokio::test]
async fn repeated_identical_object_requests_emit_one_row() {
    let (_root, pages) = source(&[(CANONICAL, b"same")]).await;
    let batches = into_stream(
        pages
            .open(FileSetRequest::new([CANONICAL, CANONICAL]))
            .await
            .unwrap(),
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    assert_eq!(batches.len(), 1);
}

#[rstest]
#[case(false)]
#[case(true)]
#[tokio::test]
async fn native_parallel_scan_handles_content_quarantine(#[case] healthy: bool) {
    use open_arrow_access_core::AccessorExt;
    use open_arrow_access_layers::ParallelReadLayer;
    use std::num::NonZeroUsize;
    const HEALTHY: &str = "ocr/part-8/part-8-3-1.txt";
    let (_root, pages) =
        source(&[(CANONICAL, b"one"), (ALIAS, b"two"), (HEALTHY, b"healthy")]).await;
    let pages = pages.layer(ParallelReadLayer::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::MIN,
    ));
    let mut keys = vec![CANONICAL, ALIAS];
    if healthy {
        keys.push(HEALTHY);
    }
    let result = into_stream(pages.open(FileSetRequest::new(keys)).await.unwrap())
        .try_collect::<Vec<_>>()
        .await;
    if healthy {
        let batches = result.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0]["data"].as_binary::<i64>().value(0), b"healthy");
    } else {
        assert!(result.unwrap_err().to_string().contains("quarantined"));
    }
}

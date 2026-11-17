mod providers;
use http::StatusCode;
use open_arrow_access_core::{Bytes, ErrorKind};
use open_arrow_access_service::biorxiv::{BiorxivService, Server};
use open_arrow_access_test_support::{byte_accessor, fixture, http::HttpFixture};

#[tokio::test]
async fn pagination_uses_observed_counts_and_jats_stays_lazy() {
    let api = HttpFixture::new();
    let content = HttpFixture::new();
    let raw = String::from_utf8(fixture!("biorxiv", "details.json").await)
        .unwrap()
        .replace("ORIGIN", content.endpoint());
    api.respond(
        "/details/biorxiv/2026-11-01/2026-11-02/0/json",
        StatusCode::OK,
        raw.into_bytes(),
    );
    content.file(
        "/paper.xml",
        Bytes::from_static(b"<article>original</article>"),
    );
    let s = BiorxivService::with_connection(providers::connection(&api));
    let page = s
        .details(Server::Biorxiv, "2026-11-01", "2026-11-02", 0)
        .await
        .unwrap();
    assert_eq!(page.next_cursor, Some(2));
    assert_eq!(page.response.value()["unknown"], "retained");
    let files = s.jats(page).unwrap();
    assert_eq!(
        files.metadata().response.value()["collection"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert!(content.requests().is_empty());
    byte_accessor::assert_read_contract(
        files,
        "paper-00000.xml".into(),
        b"<article>original</article>",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn medrxiv_empty_windows_and_validation() {
    let api = HttpFixture::new();
    let s = BiorxivService::with_connection(providers::connection(&api));
    api.respond(
        "/details/medrxiv/2026-11-01/2026-11-01/0/json",
        StatusCode::OK,
        fixture!("biorxiv", "empty-window.json").await,
    );
    assert!(
        s.details(Server::Medrxiv, "2026-11-01", "2026-11-01", 0)
            .await
            .unwrap()
            .next_cursor
            .is_none()
    );
    assert_eq!(
        s.details(Server::Biorxiv, "2026-02-30", "2026-11-01", 0)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        s.details(Server::Biorxiv, "2026-11-02", "2026-11-01", 0)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(Server::Medrxiv.bulk_bucket(), "medrxiv-src-monthly");
}

#[rstest::rstest]
#[case::wrong_cursor("bad-cursor.json")]
#[case::inconsistent_count("bad-count.json")]
#[case::api_error("bad-status.json")]
#[tokio::test]
async fn malformed_pagination_and_api_errors_fail(#[case] name: &str) {
    open_arrow_access_test_support::http::assert_resolution_cases(
        env!("CARGO_MANIFEST_DIR"),
        "biorxiv",
        "/details/biorxiv/2026-11-01/2026-11-01/0/json",
        &[(name, StatusCode::OK, ErrorKind::Decode)],
        |endpoint, context| async move {
            BiorxivService::with_connection(providers::connection_at(&endpoint, context))
                .details(Server::Biorxiv, "2026-11-01", "2026-11-01", 0)
                .await
        },
    )
    .await;
}

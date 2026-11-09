mod providers;
use http::StatusCode;
use open_arrow_access_core::{Bytes, ErrorKind};
use open_arrow_access_service::biorxiv::{BiorxivService, Server};
use open_arrow_access_test_support::{byte_accessor, http::HttpFixture};

#[tokio::test]
async fn pagination_uses_observed_counts_and_jats_stays_lazy() {
    let api = HttpFixture::new();
    let content = HttpFixture::new();
    let raw = format!(
        r#"{{"messages":[{{"status":"ok","cursor":"0","total":"3"}}],"collection":[{{"doi":"a","version":"1","license":"cc_by","jatsxml":"{}/paper.xml"}},{{"doi":"b","jatsxml":null}}],"unknown":"retained"}}"#,
        content.endpoint()
    );
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
        Bytes::from_static(br#"{"messages":[{"status":"no posts found"}],"collection":[]}"#),
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
    for body in [
        br#"{"messages":[{"status":"ok","cursor":1,"total":2}],"collection":[]}"#.as_slice(),
        br#"{"messages":[{"status":"ok","cursor":0,"total":2}],"collection":[]}"#,
        br#"{"messages":[{"status":"error"}],"collection":[]}"#,
    ] {
        api.respond(
            "/details/biorxiv/2026-11-01/2026-11-01/0/json",
            StatusCode::OK,
            Bytes::copy_from_slice(body),
        );
        assert_eq!(
            s.details(Server::Biorxiv, "2026-11-01", "2026-11-01", 0)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Decode
        );
    }
    assert_eq!(Server::Medrxiv.bulk_bucket(), "medrxiv-src-monthly");
}

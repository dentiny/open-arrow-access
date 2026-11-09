mod providers;
use http::StatusCode;
use open_arrow_access_core::{Bytes, ErrorKind};
use open_arrow_access_service::openstax::OpenStaxService;
use open_arrow_access_test_support::{byte_accessor, fixture, http::HttpFixture};

#[tokio::test]
async fn book_snapshot_pins_commit_and_defers_archive_download() {
    let api = HttpFixture::new();
    let archives = HttpFixture::new();
    api.respond(
        "/repos/openstax/osbooks-physics/commits/main",
        StatusCode::OK,
        fixture!("openstax", "commit.json").await,
    );
    archives.file(
        "/openstax/osbooks-physics/tar.gz/0123456789012345678901234567890123456789",
        Bytes::from_static(b"source snapshot"),
    );
    let service = OpenStaxService::with_connections(
        providers::connection(&api),
        providers::connection(&archives),
    );
    let files = service.book("osbooks-physics", "main").await.unwrap();
    assert_eq!(
        files.metadata().commit_sha,
        "0123456789012345678901234567890123456789"
    );
    assert!(archives.requests().is_empty());
    byte_accessor::assert_read_contract(files, "osbooks-physics.tar.gz".into(), b"source snapshot")
        .await
        .unwrap();
}

#[tokio::test]
async fn rejects_unsupported_repositories_and_unpinned_responses() {
    let f = HttpFixture::new();
    let s = OpenStaxService::with_connections(providers::connection(&f), providers::connection(&f));
    assert!(s.book("../repo", "main").await.is_err());
    assert!(f.requests().is_empty());
    f.respond(
        "/repos/openstax/osbooks-physics/commits/main",
        StatusCode::OK,
        Bytes::from_static(b"{\"sha\":\"main\"}"),
    );
    assert_eq!(
        s.book("osbooks-physics", "main").await.unwrap_err().kind(),
        ErrorKind::Decode
    );
    assert!(s.repositories(0, 10).await.is_err());
}

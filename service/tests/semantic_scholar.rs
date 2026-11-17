mod providers;
use http::StatusCode;
use open_arrow_access_core::{Bytes, ErrorKind};
use open_arrow_access_service::semantic_scholar::SemanticScholarService;
use open_arrow_access_test_support::{byte_accessor, fixture, http::HttpFixture};

#[tokio::test]
async fn pins_latest_and_preserves_signed_partitions_without_leaking_key() {
    let f = HttpFixture::new();
    let download = HttpFixture::new();
    f.respond(
        "/datasets/v1/release/latest",
        StatusCode::OK,
        fixture!("semantic-scholar", "release.json").await,
    );
    let raw = String::from_utf8(fixture!("semantic-scholar", "dataset.json").await)
        .unwrap()
        .replace("ORIGIN", download.endpoint());
    f.respond(
        "/datasets/v1/release/2026-10-27/dataset/s2orc",
        StatusCode::OK,
        raw.into_bytes(),
    );
    download.file("/part?signature=a%2Bb", Bytes::from_static(b"raw-data"));
    let s = SemanticScholarService::with_connection(
        providers::connection(&f)
            .with_header("x-api-key", "test")
            .unwrap(),
    );
    let files = s.dataset("latest", "s2orc").await.unwrap();
    assert_eq!(files.metadata().release_id, "2026-10-27");
    assert_eq!(files.metadata().dataset.value()["README"], "source license");
    assert_eq!(f.requests().len(), 2);
    assert!(download.requests().is_empty());
    byte_accessor::assert_read_contract(files, "part-00000".into(), b"raw-data")
        .await
        .unwrap();
    assert!(!download.requests()[0].headers.contains_key("x-api-key"));
}

#[tokio::test]
async fn validates_identity_and_required_arrays() {
    let f = HttpFixture::new();
    f.respond(
        "/datasets/v1/release/2026-10-27",
        StatusCode::OK,
        fixture!("semantic-scholar", "release.json").await,
    );
    f.respond(
        "/datasets/v1/release/2026-10-27/dataset/s2orc",
        StatusCode::OK,
        Bytes::from_static(b"{\"name\":\"wrong\",\"files\":[]}"),
    );
    let s = SemanticScholarService::with_connection(providers::connection(&f));
    assert_eq!(
        s.dataset("2026-10-27", "s2orc").await.unwrap_err().kind(),
        ErrorKind::Decode
    );
    assert!(SemanticScholarService::new("").is_err());
}

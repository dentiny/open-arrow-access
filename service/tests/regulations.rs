mod providers;
use http::StatusCode;
use open_arrow_access_core::{Bytes, ErrorKind};
use open_arrow_access_service::regulations::{RegulationsResource, RegulationsService};
use open_arrow_access_test_support::{byte_accessor, fixture, http::HttpFixture};

#[tokio::test]
async fn record_retains_upstream_fields_and_attachment_representations() {
    let api = HttpFixture::new();
    let downloads = HttpFixture::new();
    let raw = String::from_utf8(fixture!("regulations", "record.json").await)
        .unwrap()
        .replace("ORIGIN", downloads.endpoint());
    api.respond(
        "/v4/documents/DOC-1?include=attachments",
        StatusCode::OK,
        raw.into_bytes(),
    );
    downloads.file("/attachment.pdf", Bytes::from_static(b"%PDFsource"));
    let s = RegulationsService::with_connection(
        providers::connection(&api)
            .with_header("X-Api-Key", "test")
            .unwrap(),
    );
    let files = s
        .record(RegulationsResource::Documents, "DOC-1")
        .await
        .unwrap();
    assert_eq!(
        files.metadata().value()["data"]["attributes"]["withdrawn"],
        true
    );
    assert!(downloads.requests().is_empty());
    byte_accessor::assert_read_contract(files, "attachment-000/file-000".into(), b"%PDFsource")
        .await
        .unwrap();
    assert!(!downloads.requests()[0].headers.contains_key("x-api-key"));
}

#[tokio::test]
async fn pages_expose_truncation_metadata_and_validate_limits() {
    let f = HttpFixture::new();
    f.respond(
        "/v4/comments?page%5Bnumber%5D=20&page%5Bsize%5D=250",
        StatusCode::OK,
        fixture!("regulations", "page.json").await,
    );
    let s = RegulationsService::with_connection(providers::connection(&f));
    let page = s
        .search(RegulationsResource::Comments, &[], 20, 250)
        .await
        .unwrap();
    assert_eq!(page.value()["meta"]["totalElements"], 88061);
    assert!(
        s.search(RegulationsResource::Comments, &[], 21, 250)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn record_rejects_mismatched_identity() {
    let f = HttpFixture::new();
    let s = RegulationsService::with_connection(providers::connection(&f));
    f.respond(
        "/v4/dockets/DOC-1",
        StatusCode::OK,
        Bytes::from_static(b"{\"data\":{\"id\":\"WRONG\",\"type\":\"dockets\",\"attributes\":{}}}"),
    );
    assert_eq!(
        s.record(RegulationsResource::Dockets, "DOC-1")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Decode
    );
}

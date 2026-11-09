mod providers;
use http::StatusCode;
use open_arrow_access_core::{Bytes, ErrorKind};
use open_arrow_access_service::federal_register::FederalRegisterService;
use open_arrow_access_test_support::{byte_accessor, fixture, http::HttpFixture};

#[tokio::test]
async fn search_preserves_cursor_links_and_document_content_is_lazy() {
    let api = HttpFixture::new();
    let downloads = HttpFixture::new();
    api.respond(
        "/api/v1/documents.json?per_page=1",
        StatusCode::OK,
        fixture!("federal-register", "page.json").await,
    );
    api.respond(
        "/api/v1/documents?format=json&search_after_cursor=a%2Bb",
        StatusCode::OK,
        Bytes::from_static(b"{\"results\":[],\"next_page_url\":null}"),
    );
    let raw = String::from_utf8(fixture!("federal-register", "document.json").await)
        .unwrap()
        .replace("ORIGIN", downloads.endpoint());
    api.respond(
        "/api/v1/documents/2026-22789.json",
        StatusCode::OK,
        raw.into_bytes(),
    );
    downloads.file("/paper.pdf", Bytes::from_static(b"%PDForiginal"));
    let s = FederalRegisterService::with_connection(providers::connection(&api));
    let page = s.search(&[("per_page", "1")]).await.unwrap();
    assert_eq!(page.value()["count"], 10000);
    assert!(
        s.next_page(&format!(
            "{}/api/v1/documents?format=json&search_after_cursor=a%2Bb",
            api.endpoint()
        ))
        .await
        .unwrap()
        .value()["results"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let files = s.document("2026-22789").await.unwrap();
    assert_eq!(files.metadata().value()["unknown"], "retained");
    assert!(downloads.requests().is_empty());
    byte_accessor::assert_read_contract(files, "pdf_url".into(), b"%PDForiginal")
        .await
        .unwrap();
}

#[tokio::test]
async fn metadata_only_documents_survive_and_wrong_identity_fails() {
    let f = HttpFixture::new();
    let s = FederalRegisterService::with_connection(providers::connection(&f));
    f.respond(
        "/api/v1/documents/1.json",
        StatusCode::OK,
        Bytes::from_static(b"{\"document_number\":\"1\",\"pdf_url\":null}"),
    );
    assert_eq!(
        s.document("1").await.unwrap().metadata().value()["document_number"],
        "1"
    );
    f.respond(
        "/api/v1/documents/1.json",
        StatusCode::OK,
        Bytes::from_static(b"{\"document_number\":\"wrong\"}"),
    );
    assert_eq!(s.document("1").await.unwrap_err().kind(), ErrorKind::Decode);
}

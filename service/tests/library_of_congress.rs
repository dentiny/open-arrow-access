mod providers;
use http::StatusCode;
use open_arrow_access_core::{Bytes, ErrorKind};
use open_arrow_access_service::library_of_congress::LibraryOfCongressService;
use open_arrow_access_test_support::{byte_accessor, fixture, http::HttpFixture};

#[tokio::test]
async fn resources_keep_rights_and_resolve_all_selected_representations_lazily() {
    let api = HttpFixture::new();
    let files = HttpFixture::new();
    let raw = String::from_utf8(fixture!("library-of-congress", "resource.json").await)
        .unwrap()
        .replace("ORIGIN", files.endpoint());
    api.respond("/resource/r1/?fo=json", StatusCode::OK, raw.into_bytes());
    files.file("/book.pdf", Bytes::from_static(b"%PDFbook"));
    let s = LibraryOfCongressService::with_connections(
        providers::connection(&api),
        providers::connection(&api),
    );
    let resource = s.resource("r1").await.unwrap();
    assert_eq!(resource.metadata().value()["item"]["rights"][0], "retained");
    assert!(files.requests().is_empty());
    byte_accessor::assert_read_contract(resource, "resource/pdf".into(), b"%PDFbook")
        .await
        .unwrap();
}

#[tokio::test]
async fn legacy_item_urls_and_original_ocr_json_are_supported() {
    let api = HttpFixture::new();
    let text = HttpFixture::new();
    api.respond(
        "/item/book1/?fo=json",
        StatusCode::OK,
        Bytes::from_static(b"{\"item\":{\"title\":[\"book\"]},\"resources\":[]}"),
    );
    text.respond("/text-services/word-coordinates-service?segment=%2Fservice%2Fbook%2Fpage.xml&format=alto_xml&full_text=1",StatusCode::OK,fixture!("library-of-congress","ocr.json").await);
    let s = LibraryOfCongressService::with_connections(
        providers::connection(&api),
        providers::connection(&text),
    );
    let legacy = format!(
        "{}/item/book1/",
        api.endpoint().replace("https://", "http://")
    );
    assert_eq!(
        s.item(&legacy).await.unwrap().value()["item"]["title"][0],
        "book"
    );
    assert_eq!(
        s.ocr("/service/book/page.xml", "alto_xml")
            .await
            .unwrap()
            .value()["/service/book/page.xml"]["full_text"],
        "original OCR"
    );
    api.respond(
        "/resource/r2/?fo=json",
        StatusCode::OK,
        Bytes::from_static(b"{\"resource\":{\"files\":5}}"),
    );
    assert_eq!(
        s.resource("r2").await.unwrap_err().kind(),
        ErrorKind::Decode
    );
    assert!(s.search(&[], 0, 10).await.is_err());
}

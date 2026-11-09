mod providers;
use http::StatusCode;
use open_arrow_access_core::Bytes;
use open_arrow_access_service::oapen::OapenService;
use open_arrow_access_test_support::{fixture, http::HttpFixture};

#[tokio::test]
async fn configures_provider_endpoint_and_complete_metadata_format() {
    let f = HttpFixture::new();
    f.respond(
        "/oai/request?verb=ListRecords&metadataPrefix=xoai",
        StatusCode::OK,
        Bytes::from(fixture!("oai", "page.xml").await),
    );
    let service = OapenService::with_connection(providers::connection(&f)).unwrap();
    let page = service.records(None, None, None).await.unwrap();
    assert_eq!(page.record_count, 2);
    assert_eq!(page.resumption_token.as_deref(), Some("next+token&x"));
    assert_eq!(f.requests().len(), 1);
}

#[tokio::test]
async fn downloads_hosted_book_and_rejects_publisher_origin() {
    use open_arrow_access_core::ByteReadOptions;
    use open_arrow_access_test_support::collect_bytes;
    let f = HttpFixture::new();
    f.file("/bitstream/book.pdf", Bytes::from_static(b"%PDFbook"));
    let s = OapenService::with_connection(providers::connection(&f)).unwrap();
    let mut reader = s
        .open_book(
            &format!("{}/bitstream/book.pdf", f.endpoint()),
            ByteReadOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(collect_bytes(reader.as_mut()).await.unwrap(), b"%PDFbook");
    assert!(
        s.open_book(
            "https://publisher.invalid/book.pdf",
            ByteReadOptions::default()
        )
        .await
        .is_err()
    );
}

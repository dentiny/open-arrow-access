mod providers;
use http::StatusCode;
use open_arrow_access_core::Bytes;
use open_arrow_access_service::arxiv_metadata::ArxivMetadataService;
use open_arrow_access_test_support::{fixture, http::HttpFixture};

#[tokio::test]
async fn configures_provider_endpoint_and_complete_metadata_format() {
    let f = HttpFixture::new();
    f.respond(
        "/oai?verb=ListRecords&metadataPrefix=arXivRaw",
        StatusCode::OK,
        Bytes::from(fixture!("oai", "page.xml").await),
    );
    let service = ArxivMetadataService::with_connection(providers::connection(&f)).unwrap();
    let page = service.records(None, None, None).await.unwrap();
    assert_eq!(page.record_count, 2);
    assert_eq!(page.resumption_token.as_deref(), Some("next+token&x"));
    assert_eq!(f.requests().len(), 1);
}

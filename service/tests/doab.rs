mod providers;
use http::StatusCode;
use open_arrow_access_core::Bytes;
use open_arrow_access_service::doab::DoabService;
use open_arrow_access_test_support::{fixture, http::HttpFixture};

#[tokio::test]
async fn configures_provider_endpoint_and_complete_metadata_format() {
    let f = HttpFixture::new();
    f.respond(
        "/oai/request?verb=ListRecords&metadataPrefix=xoai",
        StatusCode::OK,
        Bytes::from(fixture!("oai", "page.xml").await),
    );
    let service = DoabService::with_connection(providers::connection(&f)).unwrap();
    let page = service.records(None, None, None).await.unwrap();
    assert_eq!(page.record_count, 2);
    assert_eq!(page.resumption_token.as_deref(), Some("next+token&x"));
    assert_eq!(f.requests().len(), 1);
}

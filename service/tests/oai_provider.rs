mod providers;
use http::StatusCode;
use open_arrow_access_service::oai::{OaiProvider, OaiProviderService};
use open_arrow_access_test_support::{fixture, http::HttpFixture};
#[derive(Clone, Debug)]
struct TestProvider;
impl OaiProvider for TestProvider {
    const ORIGIN: &'static str = "https://example.invalid";
    const PATH: &'static str = "harvest";
    const METADATA_PREFIX: &'static str = "xoai";
}

#[tokio::test]
async fn shares_provider_configuration_without_duplicating_protocol_state() {
    let f = HttpFixture::new();
    f.respond(
        "/harvest?verb=ListRecords&metadataPrefix=xoai",
        StatusCode::OK,
        fixture!("oai", "page.xml").await,
    );
    let service =
        OaiProviderService::<TestProvider>::with_connection(providers::connection(&f)).unwrap();
    let page = service.records(None, None, None).await.unwrap();
    assert_eq!(page.record_count, 2);
    assert_eq!(page.resumption_token.as_deref(), Some("next+token&x"));
    assert_eq!(f.requests().len(), 1);
    assert_eq!(
        service.connection().origin().host_str(),
        url::Url::parse(f.endpoint()).unwrap().host_str()
    );
}

// The aliases differ only in these constants. Exercise protocol/HTTP behavior once above.
#[cfg(any(feature = "doab", feature = "oapen", feature = "arxiv"))]
#[test]
fn built_in_providers_select_their_endpoint_and_metadata_format() {
    fn configured<P: OaiProvider>(origin: &str, path: &str, prefix: &str) {
        assert_eq!(
            (P::ORIGIN, P::PATH, P::METADATA_PREFIX),
            (origin, path, prefix)
        );
    }
    #[cfg(feature = "doab")]
    configured::<open_arrow_access_service::doab::Doab>(
        "https://directory.doabooks.org",
        "oai/request",
        "xoai",
    );
    #[cfg(feature = "oapen")]
    configured::<open_arrow_access_service::oapen::Oapen>(
        "https://library.oapen.org",
        "oai/request",
        "xoai",
    );
    #[cfg(feature = "arxiv")]
    configured::<open_arrow_access_service::arxiv_metadata::ArxivMetadata>(
        "https://oaipmh.arxiv.org",
        "oai",
        "arXivRaw",
    );
}

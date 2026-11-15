mod providers;
use arrow_array::{Array, StructArray, cast::AsArray};
use http::StatusCode;
use open_arrow_access_core::ReadRequest;
use open_arrow_access_service::oai::{OaiProvider, OaiProviderService};
use open_arrow_access_test_support::{fixture, format::collect_batches, http::HttpFixture};
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

fn child<'a>(record: &'a StructArray, local_name: &str) -> &'a arrow_array::ArrayRef {
    let index = record
        .fields()
        .iter()
        .position(|field| field.name().rsplit(':').next() == Some(local_name))
        .unwrap();
    record.column(index)
}

#[tokio::test]
async fn inferred_mapping_preserves_metadata_and_deleted_rows_without_extra_requests() {
    for name in ["page.xml", "prefixed-page.xml"] {
        let http = HttpFixture::new();
        http.respond(
            "/harvest?verb=ListRecords&metadataPrefix=xoai",
            StatusCode::OK,
            fixture!("oai", name).await,
        );
        let service =
            OaiProviderService::<TestProvider>::with_connection(providers::connection(&http))
                .unwrap();
        let page = service.records(None, None, None).await.unwrap();
        let batches = collect_batches(
            &page.records(service.inferred_record_format()),
            ReadRequest::new("oai-page"),
        )
        .await;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        let records = StructArray::from(batches[0].clone());
        let header = child(&records, "header").as_struct();
        assert_eq!(
            child(header, "identifier").as_string::<i32>().value(0),
            "id1"
        );
        let metadata = child(&records, "metadata").as_struct();
        assert!(metadata.is_null(0));
        let raw = child(metadata, "raw").as_struct();
        assert_eq!(
            child(raw, "license").as_string::<i32>().value(1),
            "all rights"
        );
        assert_eq!(http.requests().len(), 1);
    }
}

mod providers;
use arrow_array::{Array, cast::AsArray};
use arrow_schema::DataType;
use open_arrow_access_core::{Accessor, Bytes, ReadRequest};

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

#[tokio::test]
async fn default_format_maps_headers_and_deleted_records() {
    let f = HttpFixture::new();
    f.respond(
        "/harvest?verb=ListRecords&metadataPrefix=xoai",
        StatusCode::OK,
        fixture!("oai", "headers.xml").await,
    );
    let service =
        OaiProviderService::<TestProvider>::with_connection(providers::connection(&f)).unwrap();
    let page = service.records(None, None, None).await.unwrap();
    let mut reader = page
        .records(service.record_format())
        .open(ReadRequest::new("headers"))
        .await
        .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 4);
    // The fixture uses a different namespace prefix and contains a metadata
    // identifier that must not be confused with the OAI header identifier.
    assert_eq!(batch["identifier"].as_string::<i32>().value(0), "001&A");
    assert_eq!(batch["identifier"].as_string::<i32>().value(1), "gone");
    assert_eq!(batch["datestamp"].as_string::<i32>().value(0), "2026-11-01");
    assert!(batch["status"].is_null(0));
    assert_eq!(batch["status"].as_string::<i32>().value(1), "deleted");
    let sets = batch["set_specs"].as_list::<i32>();
    assert_eq!(
        sets.value(0).as_string::<i32>().iter().collect::<Vec<_>>(),
        vec![
            Some("books:science"),
            Some("books:all"),
            Some("books:science")
        ]
    );
    assert!(!sets.is_null(1));
    assert!(sets.value(1).is_empty());
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn default_schema_is_stable_for_deleted_and_empty_pages() {
    let f = HttpFixture::new();
    let service =
        OaiProviderService::<TestProvider>::with_connection(providers::connection(&f)).unwrap();
    let cases = [
        (Bytes::from(fixture!("oai", "headers.xml").await), 2),
        (Bytes::from_static(br#"<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"><ListRecords><record><header status="deleted"><identifier>gone</identifier></header></record></ListRecords></OAI-PMH>"#), 1),
        (Bytes::from_static(br#"<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"><ListRecords><resumptionToken/></ListRecords></OAI-PMH>"#), 0),
        (Bytes::from(fixture!("oai", "empty.xml").await), 0),
    ];
    let mut expected_schema = None;
    for (bytes, count) in cases {
        f.respond(
            "/harvest?verb=ListRecords&metadataPrefix=xoai",
            StatusCode::OK,
            bytes,
        );
        let page = service.records(None, None, None).await.unwrap();
        let mut reader = page
            .records(service.record_format())
            .open(ReadRequest::new("page"))
            .await
            .unwrap();
        let schema = reader.metadata().schema.clone();
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        if let Some(expected) = &expected_schema {
            assert_eq!(&schema, expected);
        } else {
            expected_schema = Some(schema);
        }
        let mut rows = 0;
        while let Some(batch) = reader.next().await.unwrap() {
            if count == 1 {
                assert!(batch["datestamp"].is_null(0));
            }
            rows += batch.num_rows();
        }
        assert_eq!(rows, count);
    }
}

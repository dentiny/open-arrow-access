mod providers;
use std::num::NonZeroUsize;

use arrow_array::{Array, cast::AsArray};
use arrow_schema::DataType;
use open_arrow_access_core::{Accessor, Bytes, ReadOptions, ReadRequest, Reader, ScanPlanner};
use open_arrow_access_file_format::xml::{NamespaceBinding, XmlField, XmlFormat, XmlSchema};

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
async fn default_format_reopens_deleted_headers_without_refetching_the_page() {
    let f = HttpFixture::new();
    let original = Bytes::from(fixture!("oai", "page.xml").await);
    f.respond(
        "/harvest?verb=ListRecords&metadataPrefix=xoai",
        StatusCode::OK,
        original.clone(),
    );
    let service =
        OaiProviderService::<TestProvider>::with_connection(providers::connection(&f)).unwrap();
    let page = service.records(None, None, None).await.unwrap();
    let records = page.records(service.record_format());
    let request = ReadRequest::new("oai-page");
    // An already fetched page can be reopened without another protocol request.
    for _ in 0..2 {
        let mut reader = records.open(request.clone()).await.unwrap();
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch["identifier"].as_string::<i32>().value(0), "id1");
        assert_eq!(batch["identifier"].as_string::<i32>().value(1), "id2");
        assert_eq!(batch["status"].as_string::<i32>().value(0), "deleted");
        assert!(batch["status"].is_null(1));
        assert_eq!(batch["datestamp"].null_count(), 2);
        let sets = batch["set_specs"].as_list::<i32>();
        assert!(!sets.is_null(0));
        assert!(sets.value(0).is_empty());
        assert!(reader.next().await.unwrap().is_none());
    }
    assert_eq!(page.bytes, original);
    assert_eq!(page.resumption_token.as_deref(), Some("next+token&x"));
    assert_eq!(f.requests().len(), 1);
}

#[tokio::test]
async fn default_format_matches_namespace_uris_and_preserves_strings_and_set_order() {
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
    assert_eq!(batch["identifier"].as_string::<i32>().value(0), "001&A");
    assert_eq!(batch["datestamp"].as_string::<i32>().value(0), "2026-11-01");
    assert_eq!(
        batch["datestamp"].as_string::<i32>().value(1),
        "2026-11-02T01:02:03Z"
    );
    let sets = batch["set_specs"].as_list::<i32>().value(0);
    assert_eq!(
        sets.as_string::<i32>().iter().collect::<Vec<_>>(),
        vec![
            Some("books:science"),
            Some("books:all"),
            Some("books:science")
        ]
    );
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn default_schema_is_stable_for_deleted_only_pages_and_both_empty_responses() {
    let f = HttpFixture::new();
    let service =
        OaiProviderService::<TestProvider>::with_connection(providers::connection(&f)).unwrap();
    let cases = [
        (Bytes::from(fixture!("oai", "page.xml").await), 2),
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
        assert_eq!(page.record_count, count);
        let records = page.records(service.record_format());
        let request = ReadRequest::new("page");
        let mut reader = records.open(request.clone()).await.unwrap();
        let schema = reader.metadata().schema.clone();
        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).name(), "identifier");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        if let Some(expected) = &expected_schema {
            assert_eq!(&schema, expected);
        } else {
            expected_schema = Some(schema.clone());
        }
        let mut rows = 0;
        while let Some(batch) = reader.next().await.unwrap() {
            rows += batch.num_rows();
        }
        assert_eq!(rows, count);
        let plan = records.plan_scan(request).await.unwrap();
        assert_eq!(plan.metadata.schema, schema);
        let mut planned = plan.into_reader();
        let mut rows = 0;
        while let Some(batch) = planned.next().await.unwrap() {
            rows += batch.num_rows();
        }
        assert_eq!(rows, count);
    }
    assert_eq!(f.requests().len(), 4);
}

#[tokio::test]
async fn default_format_supports_projection_and_single_row_batches() {
    let f = HttpFixture::new();
    f.respond(
        "/harvest?verb=ListRecords&metadataPrefix=xoai",
        StatusCode::OK,
        fixture!("oai", "page.xml").await,
    );
    let service =
        OaiProviderService::<TestProvider>::with_connection(providers::connection(&f)).unwrap();
    let page = service.records(None, None, None).await.unwrap();
    for projection in [vec!["status".to_owned(), "identifier".to_owned()], vec![]] {
        let mut reader = page
            .records(service.record_format())
            .open(ReadRequest::new("page").with_options(ReadOptions {
                projection: Some(projection.clone()),
                batch_size: NonZeroUsize::MIN,
            }))
            .await
            .unwrap();
        assert_eq!(
            reader
                .metadata()
                .schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>(),
            projection
        );
        let mut rows = 0;
        while let Some(batch) = reader.next().await.unwrap() {
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), projection.len());
            rows += batch.num_rows();
        }
        assert_eq!(rows, 2);
    }
}

#[derive(Clone, Debug)]
struct MetadataProvider;
impl OaiProvider for MetadataProvider {
    const ORIGIN: &'static str = TestProvider::ORIGIN;
    const PATH: &'static str = TestProvider::PATH;
    const METADATA_PREFIX: &'static str = TestProvider::METADATA_PREFIX;

    fn record_format() -> XmlFormat {
        let schema = XmlSchema::with_namespaces(
            vec![XmlField::new(
                "title",
                "oai:metadata/book:book/book:title",
                DataType::Utf8,
                true,
            )],
            &[
                NamespaceBinding {
                    prefix: "oai",
                    uri: "http://www.openarchives.org/OAI/2.0/",
                },
                NamespaceBinding {
                    prefix: "book",
                    uri: "urn:books",
                },
            ],
        )
        .unwrap();
        TestProvider::record_format().with_schema(schema)
    }
}

#[tokio::test]
async fn provider_override_can_select_metadata_with_its_own_namespace() {
    let f = HttpFixture::new();
    let bytes = Bytes::from(fixture!("oai", "headers.xml").await);
    f.respond(
        "/harvest?verb=ListRecords&metadataPrefix=xoai",
        StatusCode::OK,
        bytes.clone(),
    );
    let service =
        OaiProviderService::<MetadataProvider>::with_connection(providers::connection(&f)).unwrap();
    let page = service.records(None, None, None).await.unwrap();
    let mut reader = page
        .records(service.record_format())
        .open(ReadRequest::new("metadata"))
        .await
        .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch["title"].as_string::<i32>().value(0), "A book");
    assert!(batch["title"].is_null(1));
    assert_eq!(page.bytes, bytes);
    assert_eq!(f.requests().len(), 1);
}

#[tokio::test]
async fn built_in_providers_share_the_default_protocol_mapping() {
    let f = HttpFixture::new();
    f.respond(
        "/harvest?verb=ListRecords&metadataPrefix=xoai",
        StatusCode::OK,
        fixture!("oai", "page.xml").await,
    );
    let service =
        OaiProviderService::<TestProvider>::with_connection(providers::connection(&f)).unwrap();
    let page = service.records(None, None, None).await.unwrap();
    let formats = vec![
        TestProvider::record_format(),
        #[cfg(feature = "doab")]
        open_arrow_access_service::doab::Doab::record_format(),
        #[cfg(feature = "oapen")]
        open_arrow_access_service::oapen::Oapen::record_format(),
        #[cfg(feature = "arxiv")]
        open_arrow_access_service::arxiv_metadata::ArxivMetadata::record_format(),
    ];
    let mut schema = None;
    for format in formats {
        let mut reader = page
            .records(format)
            .open(ReadRequest::new("builtin"))
            .await
            .unwrap();
        if let Some(expected) = &schema {
            assert_eq!(&reader.metadata().schema, expected);
        } else {
            schema = Some(reader.metadata().schema.clone());
        }
        assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 2);
    }
}

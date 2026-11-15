mod providers;
use arrow_array::{Array, RecordBatch, StructArray, cast::AsArray};
use http::StatusCode;
use open_arrow_access_core::{Accessor, ReadOptions, ReadRequest};
use open_arrow_access_file_format::xml::{NamespaceBinding, XmlFormat};
use open_arrow_access_service::oai::OaiPage;
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

async fn decode(page: &OaiPage, format: XmlFormat) -> Vec<RecordBatch> {
    let mut reader = page
        .records(format)
        .open(ReadRequest::new("oai-page").with_options(ReadOptions {
            batch_size: std::num::NonZeroUsize::new(1).unwrap(),
            ..Default::default()
        }))
        .await
        .unwrap();
    let mut batches = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        batches.push(batch);
    }
    batches
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
    for prefixed in [false, true] {
        let http = HttpFixture::new();
        let mut xml = String::from_utf8(fixture!("oai", "page.xml").await).unwrap();
        if prefixed {
            xml = xml.replace("xmlns=", "xmlns:source=");
            for element in [
                "OAI-PMH",
                "ListRecords",
                "record",
                "header",
                "identifier",
                "metadata",
                "raw",
                "license",
                "resumptionToken",
            ] {
                xml = xml
                    .replace(&format!("<{element}"), &format!("<source:{element}"))
                    .replace(&format!("</{element}"), &format!("</source:{element}"));
            }
        }
        http.respond(
            "/harvest?verb=ListRecords&metadataPrefix=xoai",
            StatusCode::OK,
            xml.clone(),
        );
        let service =
            OaiProviderService::<TestProvider>::with_connection(providers::connection(&http))
                .unwrap();
        let page = service.records(None, None, None).await.unwrap();
        let batches = decode(&page, service.inferred_record_format()).await;
        // This is the mapping currently repeated by downstream DOAB/OAPEN readers.
        let previous =
            XmlFormat::new("/oai:OAI-PMH/oai:ListRecords/oai:record").with_namespaces(&[
                NamespaceBinding {
                    prefix: "oai",
                    uri: "http://www.openarchives.org/OAI/2.0/",
                },
            ]);
        assert_eq!(batches, decode(&page, previous).await);
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        let first = StructArray::from(batches[0].clone());
        let header = child(&first, "header").as_struct();
        assert_eq!(
            child(header, "identifier").as_string::<i32>().value(0),
            "id1"
        );
        assert!(child(&first, "metadata").is_null(0));
        let second = StructArray::from(batches[1].clone());
        let metadata = child(&second, "metadata").as_struct();
        let raw = child(metadata, "raw").as_struct();
        assert_eq!(
            child(raw, "license").as_string::<i32>().value(0),
            "all rights"
        );
        assert_eq!(page.bytes.as_ref(), xml.as_bytes());
        assert_eq!(page.resumption_token.as_deref(), Some("next+token&x"));
        assert_eq!(http.requests().len(), 1);
        // The existing fixed header mapping remains a separate choice.
        let headers = decode(&page, service.record_format()).await;
        assert_eq!(
            headers[0]
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            ["identifier", "datestamp", "status", "set_specs"]
        );
    }
}

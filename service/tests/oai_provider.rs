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

#[cfg(feature = "arxiv")]
#[tokio::test]
async fn arxiv_raw_keeps_string_ids_after_sample_boundary_and_stable_optional_lists() {
    use open_arrow_access_core::Bytes;
    use open_arrow_access_service::{arxiv_metadata::ArxivMetadataService, oai::OaiPage};
    let service = ArxivMetadataService::new().unwrap();
    let mut xml = String::from(
        r#"<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:a="http://arxiv.org/OAI/arXivRaw/"><ListRecords>"#,
    );
    let ids: Vec<_> = (0..129)
        .map(|n| format!("2410.{n:05}"))
        .chain(["cond-mat/0604352".into(), "0001.0123".into()])
        .collect();
    for id in &ids {
        xml.push_str(&format!(r#"<record><header><identifier>oai:arXiv.org:{id}</identifier><datestamp>2026-11-02</datestamp><setSpec>physics:cond-mat</setSpec><setSpec>physics</setSpec></header><metadata><a:arXivRaw><a:id>{id}</a:id><a:submitter>Sample</a:submitter><a:version version="v1"><a:date>2006-01-01</a:date><a:size>21kb</a:size></a:version><a:version version="v2"><a:date>2026-11-02</a:date><a:source_type>D</a:source_type></a:version><a:title>00123</a:title><a:authors>Test Author</a:authors><a:categories>cond-mat</a:categories><a:doi>001</a:doi><a:abstract>Text</a:abstract></a:arXivRaw></metadata></record>"#));
    }
    xml.push_str(r#"<record><header status="deleted"><identifier>oai:arXiv.org:2410.99999</identifier><datestamp>2026-11-02</datestamp></header></record></ListRecords></OAI-PMH>"#);
    let page = OaiPage {
        bytes: Bytes::from(xml),
        record_count: ids.len() + 1,
        resumption_token: None,
    };
    let batches = collect_batches(
        &page.records(service.inferred_record_format()),
        ReadRequest::new("arxiv"),
    )
    .await;
    assert_eq!(batches.len(), 1);
    let records = StructArray::from(batches[0].clone());
    let metadata = child(&records, "metadata").as_struct();
    assert!(metadata.is_null(ids.len()));
    let raw = child(metadata, "arXivRaw").as_struct();
    let actual = child(raw, "id").as_string::<i32>();
    for (row, id) in ids.iter().enumerate() {
        assert_eq!(actual.value(row), id);
    }
    assert_eq!(child(raw, "title").as_string::<i32>().value(0), "00123");
    assert_eq!(child(raw, "doi").as_string::<i32>().value(0), "001");
    let versions = child(raw, "version").as_list::<i32>().value(0);
    assert_eq!(versions.len(), 2);
    let versions = versions.as_struct();
    assert_eq!(
        child(versions, "@version").as_string::<i32>().value(1),
        "v2"
    );
    assert_eq!(
        child(versions, "source_type").as_string::<i32>().value(1),
        "D"
    );
    assert!(child(versions, "source_type").is_null(0));
    let header = child(&records, "header").as_struct();
    assert_eq!(child(header, "setSpec").as_list::<i32>().value(0).len(), 2);
    assert_eq!(
        child(header, "@status").as_string::<i32>().value(ids.len()),
        "deleted"
    );
    // Empty windows and populated/deleted pages expose the same full schema.
    let empty = OaiPage {
        bytes: Bytes::from_static(
            b"<OAI-PMH xmlns=\"http://www.openarchives.org/OAI/2.0/\"><ListRecords/></OAI-PMH>",
        ),
        record_count: 0,
        resumption_token: None,
    };
    use open_arrow_access_core::Accessor;
    let reader = empty
        .records(service.inferred_record_format())
        .open(ReadRequest::new("empty"))
        .await
        .unwrap();
    assert_eq!(reader.metadata().schema, batches[0].schema());
}

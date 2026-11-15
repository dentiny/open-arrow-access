use std::{collections::HashMap, num::NonZeroUsize};

use arrow_array::cast::AsArray;
use http::StatusCode;
use open_arrow_access_core::{
    Accessor, AccessorExt, ErrorKind, ReadOptions, ReadRequest, ScanPlanner,
};
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_service::oai::{OaiHarvestRequest, OaiProvider, OaiProviderService};
use open_arrow_access_test_support::http::{HttpFixture, response};

mod providers;

#[derive(Clone, Debug)]
struct TestProvider;
impl OaiProvider for TestProvider {
    const ORIGIN: &'static str = "https://example.invalid";
    const PATH: &'static str = "harvest";
    const METADATA_PREFIX: &'static str = "xoai";
}

fn service(http: &HttpFixture) -> OaiProviderService<TestProvider> {
    OaiProviderService::with_connection(providers::connection(http)).unwrap()
}

fn page(ids: &[&str], token: Option<&str>) -> String {
    let mut xml =
        String::from("<OAI-PMH xmlns=\"http://www.openarchives.org/OAI/2.0/\"><ListRecords>");
    for id in ids {
        let status = if *id == "deleted" {
            " status=\"deleted\""
        } else {
            ""
        };
        xml.push_str(&format!(
            "<record><header{status}><identifier>{id}</identifier></header></record>"
        ));
    }
    if let Some(token) = token {
        xml.push_str(&format!(
            "<resumptionToken>{}</resumptionToken>",
            token.replace('&', "&amp;").replace('<', "&lt;")
        ));
    }
    xml.push_str("</ListRecords></OAI-PMH>");
    xml
}

fn request() -> OaiHarvestRequest {
    OaiHarvestRequest::new(ReadRequest::new("harvest").with_options(ReadOptions {
        batch_size: NonZeroUsize::MIN,
        ..Default::default()
    }))
}

#[tokio::test]
async fn follows_opaque_tokens_lazily_across_empty_pages_and_preserves_deleted_rows() {
    let http = HttpFixture::new();
    http.handle(|request| {
        assert_eq!(request.headers()["x-api-key"], "secret");
        let query = url::form_urlencoded::parse(request.uri().query().unwrap().as_bytes())
            .into_owned()
            .collect::<HashMap<_, _>>();
        assert_eq!(query["verb"], "ListRecords");
        let xml = match query.get("resumptionToken").map(String::as_str) {
            None => {
                assert_eq!(query.len(), 5);
                assert_eq!(query["metadataPrefix"], "xoai");
                assert_eq!(query["from"], "2026-01-01");
                assert_eq!(query["until"], "2026-02-01");
                assert_eq!(query["set"], "books");
                page(&["first", "deleted"], Some(" next+token&x "))
            }
            Some(" next+token&x ") => {
                assert_eq!(query.len(), 2);
                page(&[], Some("last"))
            }
            Some("last") => {
                assert_eq!(query.len(), 2);
                page(&["third"], Some(""))
            }
            _ => panic!("unexpected token"),
        };
        Ok(response(StatusCode::OK, xml))
    });
    let provider = OaiProviderService::<TestProvider>::with_connection(
        providers::connection(&http)
            .with_header("x-api-key", "secret")
            .unwrap(),
    )
    .unwrap();
    let harvest = provider.harvest();
    assert!(http.requests().is_empty());
    let mut req = request();
    req.from = Some("2026-01-01".into());
    req.until = Some("2026-02-01".into());
    req.set = Some("books".into());
    let mut reader = harvest.open(req.clone()).await.unwrap();
    assert_eq!(http.requests().len(), 1);
    assert_eq!(
        reader.next().await.unwrap().unwrap()["identifier"]
            .as_string::<i32>()
            .value(0),
        "first"
    );
    let deleted = reader.next().await.unwrap().unwrap();
    assert_eq!(deleted["status"].as_string::<i32>().value(0), "deleted");
    assert_eq!(http.requests().len(), 1);
    assert_eq!(
        reader.next().await.unwrap().unwrap()["identifier"]
            .as_string::<i32>()
            .value(0),
        "third"
    );
    assert_eq!(http.requests().len(), 3);
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
    let independent = harvest.open(req).await.unwrap();
    drop(independent);
    assert_eq!(http.requests().len(), 4);
}

#[tokio::test]
async fn cycles_limits_and_http_errors_are_terminal_after_delivered_rows() {
    for case in ["cycle", "limit", "http", "protocol"] {
        let http = HttpFixture::new();
        http.respond(
            "/harvest?verb=ListRecords&metadataPrefix=xoai",
            StatusCode::OK,
            page(&["first"], Some("a")),
        );
        http.respond(
            "/harvest?verb=ListRecords&resumptionToken=a",
            if case == "http" {
                StatusCode::FORBIDDEN
            } else {
                StatusCode::OK
            },
            if case == "protocol" {
                "<OAI-PMH xmlns=\"http://www.openarchives.org/OAI/2.0/\"><error code=\"badResumptionToken\"/></OAI-PMH>".into()
            } else { page(&["second"], Some("a")) },
        );
        let mut req = request();
        if case == "limit" {
            req.max_pages = NonZeroUsize::new(1);
        }
        let mut reader = service(&http).harvest().open(req).await.unwrap();
        assert!(reader.next().await.unwrap().is_some());
        if case == "cycle" {
            assert!(reader.next().await.unwrap().is_some());
        }
        let error = reader.next().await.unwrap_err();
        assert_eq!(
            error.kind(),
            match case {
                "http" => ErrorKind::PermissionDenied,
                "limit" => ErrorKind::ResourceExhausted,
                _ => ErrorKind::Decode,
            }
        );
        if case == "http" {
            assert_eq!(error.http_status(), Some(403));
        }
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(http.requests().len(), if case == "limit" { 1 } else { 2 });
    }
}

#[tokio::test]
async fn initial_resume_projection_and_single_partition_planning_share_the_same_reader() {
    let http = HttpFixture::new();
    http.respond(
        "/harvest?verb=ListRecords&resumptionToken=start",
        StatusCode::OK,
        page(&["one"], Some("finish")),
    );
    http.respond(
        "/harvest?verb=ListRecords&resumptionToken=finish",
        StatusCode::OK,
        page(&["two"], None),
    );
    let harvest = service(&http).harvest();
    let mut req = request();
    req.resumption_token = Some("start".into());
    req.read.options.projection = Some(vec![]);
    let plan = harvest.plan_scan(req.clone()).await.unwrap();
    assert_eq!(plan.partitions.size_hint(), (1, Some(1)));
    assert_eq!(http.requests().len(), 1);
    drop(plan);
    let mut reader = harvest
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(4).unwrap(),
            NonZeroUsize::MIN,
        ))
        .open(req)
        .await
        .unwrap();
    let mut count = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.num_columns(), 0);
        count += batch.num_rows();
    }
    assert_eq!(count, 2);
    assert_eq!(http.requests().len(), 3);
}

#[tokio::test]
async fn validates_initial_configuration_before_network_and_keeps_empty_schema() {
    let http = HttpFixture::new();
    let harvest = service(&http).harvest();
    let mut invalid = request();
    invalid.resumption_token = Some("start".into());
    invalid.set = Some("books".into());
    assert_eq!(
        harvest.open(invalid).await.err().unwrap().kind(),
        ErrorKind::InvalidInput
    );
    let mut invalid = request();
    invalid.read.options.projection = Some(vec!["missing".into()]);
    assert_eq!(
        harvest.open(invalid).await.err().unwrap().kind(),
        ErrorKind::InvalidInput
    );
    assert!(http.requests().is_empty());
    http.respond("/harvest?verb=ListRecords&metadataPrefix=xoai", StatusCode::OK,
        "<OAI-PMH xmlns=\"http://www.openarchives.org/OAI/2.0/\"><error code=\"noRecordsMatch\"/></OAI-PMH>");
    let mut reader = harvest.open(request()).await.unwrap();
    assert_eq!(reader.metadata().schema.fields().len(), 4);
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(http.requests().len(), 1);
}

#[tokio::test]
async fn inferred_page_schema_changes_fail_before_emitting_incompatible_rows() {
    let http = HttpFixture::new();
    http.respond(
        "/harvest?verb=ListRecords&metadataPrefix=xoai",
        StatusCode::OK,
        page(&["first"], Some("next")),
    );
    let next = page(&["second"], None).replace(
        "</record>",
        "<metadata><new_field>value</new_field></metadata></record>",
    );
    http.respond(
        "/harvest?verb=ListRecords&resumptionToken=next",
        StatusCode::OK,
        next,
    );
    let provider = service(&http);
    let mut reader = provider
        .harvest()
        .with_format(provider.inferred_record_format())
        .open(request())
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(http.requests().len(), 2);
}

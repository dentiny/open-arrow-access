use std::num::NonZeroUsize;

use arrow_array::cast::AsArray;
use http::StatusCode;
use open_arrow_access_core::{
    Accessor, AccessorExt, ErrorKind, ReadOptions, ReadRequest, ScanPlanner,
};
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_service::oai::{OaiHarvestRequest, OaiProvider, OaiProviderService};
use open_arrow_access_test_support::http::HttpFixture;

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

// All routes use the same verb; None denotes the initial request.
fn respond(http: &HttpFixture, token: Option<&str>, xml: String) {
    let query = match token {
        Some(token) => format!("resumptionToken={token}"),
        None => "metadataPrefix=xoai".into(),
    };
    http.respond(
        &format!("/harvest?verb=ListRecords&{query}"),
        StatusCode::OK,
        xml,
    );
}

#[tokio::test]
async fn follows_tokens_lazily_across_empty_pages() {
    let http = HttpFixture::new();
    http.respond(
        "/harvest?verb=ListRecords&metadataPrefix=xoai&from=2026-01-01&until=2026-02-01&set=books",
        StatusCode::OK,
        page(&["first", "deleted"], Some(" next+token&x ")),
    );
    respond(&http, Some("+next%2Btoken%26x+"), page(&[], Some("last")));
    respond(&http, Some("last"), page(&["third"], Some("")));
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
    for (column, value, requests) in [
        ("identifier", "first", 1),
        ("status", "deleted", 1),
        ("identifier", "third", 3),
    ] {
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch[column].as_string::<i32>().value(0), value);
        assert_eq!(http.requests().len(), requests);
    }
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
    drop(harvest.open(req).await.unwrap());
    assert_eq!(http.requests().len(), 4);
    assert!(
        http.requests()
            .iter()
            .all(|r| r.headers["x-api-key"] == "secret")
    );
}

#[tokio::test]
async fn continuation_failures_are_terminal() {
    use ErrorKind::{Decode, PermissionDenied, ResourceExhausted};
    for (case, kind, rows, requests) in [
        ("cycle", Decode, 2, 2),
        ("limit", ResourceExhausted, 1, 1),
        ("http", PermissionDenied, 1, 2),
        ("protocol", Decode, 1, 2),
    ] {
        let http = HttpFixture::new();
        respond(&http, None, page(&["first"], Some("a")));
        let xml = if case == "protocol" {
            "<OAI-PMH><error code=\"badResumptionToken\"/></OAI-PMH>".into()
        } else {
            page(&["second"], Some("a"))
        };
        respond(&http, Some("a"), xml);
        if case == "http" {
            http.respond(
                "/harvest?verb=ListRecords&resumptionToken=a",
                StatusCode::FORBIDDEN,
                "",
            );
        }
        let mut req = request();
        if case == "limit" {
            req.max_pages = NonZeroUsize::new(1);
        }
        let mut reader = service(&http).harvest().open(req).await.unwrap();
        for _ in 0..rows {
            assert!(reader.next().await.unwrap().is_some(), "{case}");
        }
        let error = reader.next().await.unwrap_err();
        assert_eq!(error.kind(), kind, "{case}");
        if case == "http" {
            assert_eq!(error.http_status(), Some(403));
        }
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(http.requests().len(), requests, "{case}");
    }
}

#[tokio::test]
async fn resume_projection_and_planning() {
    let http = HttpFixture::new();
    respond(&http, Some("start"), page(&["one"], Some("finish")));
    respond(&http, Some("finish"), page(&["two"], None));
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

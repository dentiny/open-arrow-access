use arrow_array::cast::AsArray;
use futures::StreamExt;
use open_arrow_access_core::{Accessor, ByteReadOptions, Bytes, ErrorKind, ScanPlanner};
use open_arrow_access_service::arxiv::{
    ArxivArticle, ArxivArticleFormat as Format, ArxivArticleReadRequest, ArxivService,
};
use open_arrow_access_test_support::{MemoryAccessor, fixture};
use std::num::NonZeroUsize;

async fn load(name: &str) -> Vec<u8> {
    fixture!("arxiv-articles", name).await
}

#[test]
fn article_routes_accept_modern_ids_and_versions_without_path_injection() {
    for id in ["0704.0001", "2410.00001", "2410.00001v2"] {
        for (format, prefix) in [
            (Format::Pdf, "pdf"),
            (Format::Latex, "src"),
            (Format::Html, "html"),
        ] {
            assert_eq!(
                ArxivArticle::new(id, format).locator().unwrap(),
                format!("{prefix}/{id}")
            );
        }
    }
    for id in [
        "",
        "../2410.00001",
        "2410.00001?x=1",
        "2410.00001#f",
        "2410.00001v0",
        "2410.00001v01",
        "2410.00001v",
        "hep-th9901001",
        "hep-th/9901001v1",
        "solv-int/9901001",
        "2410.00001/extra",
    ] {
        assert!(
            ArxivArticle::new(id, Format::Html).locator().is_err(),
            "{id}"
        );
    }
}

#[tokio::test]
async fn explicit_formats_have_independent_lazy_partitions_and_preserve_payloads() {
    // Storage interprets routes; the reader never probes S3 manifests or other formats.
    let original = load("paper.html").await;
    let storage = MemoryAccessor::new(&original, 3);
    let observed = storage.observations.clone();
    let accessor = ArxivService::new(storage).article_records();
    let mut request = ArxivArticleReadRequest::new(vec![
        ArxivArticle::new("2410.00001v2", Format::Html),
        ArxivArticle::new("2410.00001v2", Format::Pdf),
        ArxivArticle::new("2410.00001v2", Format::Latex),
    ]);
    request.byte_options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        ..Default::default()
    };
    let mut plan = accessor.plan_scan(request).await.unwrap();
    assert!(observed.requests.lock().unwrap().is_empty());
    let mut count = 0;
    while let Some(partition) = plan.partitions.next().await {
        let mut reader = partition.unwrap().await.unwrap();
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(
            batch["article_id"].as_string::<i64>().value(0),
            "2410.00001v2"
        );
        assert_eq!(batch["data"].as_binary::<i64>().value(0), original);
        assert!(reader.next().await.unwrap().is_none());
        count += 1;
    }
    assert_eq!(count, 3);
    let requests = observed.requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .map(|r| r.object.locator())
            .collect::<Vec<_>>(),
        vec!["html/2410.00001v2", "pdf/2410.00001v2", "src/2410.00001v2"]
    );
    assert!(requests.iter().all(|r| r.options.io_concurrency.get() == 3));
}

#[tokio::test]
async fn invalid_selection_and_projection_fail_before_any_storage_io() {
    let storage = MemoryAccessor::new(&load("paper.pdf").await, 2);
    let observed = storage.observations.clone();
    let accessor = ArxivService::new(storage).article_records();
    let request = ArxivArticleReadRequest::new(vec![
        ArxivArticle::new("2410.00001", Format::Pdf),
        ArxivArticle::new("../bad", Format::Html),
    ]);
    assert!(accessor.plan_scan(request).await.is_err());
    let mut request =
        ArxivArticleReadRequest::new(vec![ArxivArticle::new("2410.00001", Format::Pdf)]);
    request.options.projection = Some(vec!["missing".into()]);
    assert!(accessor.open(request).await.is_err());
    assert!(observed.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn article_zero_column_projection_preserves_row_count() {
    let accessor =
        ArxivService::new(MemoryAccessor::new(&load("paper.pdf").await, 2)).article_records();
    let mut request =
        ArxivArticleReadRequest::new(vec![ArxivArticle::new("2410.00001", Format::Pdf)]);
    request.options.projection = Some(vec![]);
    let mut reader = accessor.open(request).await.unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!((batch.num_columns(), batch.num_rows()), (0, 1));
}

#[tokio::test]
async fn http_formats_preserve_distinct_bytes_and_missing_html_does_not_fallback() {
    use http::StatusCode;
    use open_arrow_access_storage::OpenDalAccessor;
    use open_arrow_access_test_support::http::HttpFixture;
    let http = HttpFixture::new();
    for (route, fixture) in [
        ("pdf", "paper.pdf"),
        ("src", "source.tar.gz"),
        ("html", "paper.html"),
    ] {
        http.file(
            &format!("/{route}/2410.00001v1"),
            Bytes::from(load(fixture).await),
        );
    }
    http.respond_for(
        http::Method::HEAD,
        "/html/2410.00002v1",
        StatusCode::NOT_FOUND,
        Bytes::new(),
    );
    http.respond("/html/2410.00002v1", StatusCode::NOT_FOUND, Bytes::new());
    let operator =
        opendal::Operator::new(opendal_service_http::Http::default().endpoint(http.endpoint()))
            .unwrap()
            .with_context(http.context());
    let accessor = ArxivService::new(OpenDalAccessor::new(operator)).article_records();
    let selections = vec![
        ArxivArticle::new("2410.00001v1", Format::Pdf),
        ArxivArticle::new("2410.00001v1", Format::Latex),
        ArxivArticle::new("2410.00001v1", Format::Html),
    ];
    let mut reader = accessor
        .open(ArxivArticleReadRequest::new(selections))
        .await
        .unwrap();
    for name in ["paper.pdf", "source.tar.gz", "paper.html"] {
        let expected = load(name).await;
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch["data"].as_binary::<i64>().value(0), expected);
    }
    assert!(reader.next().await.unwrap().is_none());
    http.clear_requests();
    let mut reader = accessor
        .open(ArxivArticleReadRequest::new(vec![ArxivArticle::new(
            "2410.00002v1",
            Format::Html,
        )]))
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::NotFound);
    assert!(
        http.requests()
            .iter()
            .all(|r| r.path == "/html/2410.00002v1")
    );
}

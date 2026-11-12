#![cfg(feature = "service-huggingface")]
use http::StatusCode;
use open_arrow_access::{
    Bytes,
    source::{SourceRegistry, SourceRequest},
};
use open_arrow_access_test_support::{
    fixture,
    http::{HttpFixture, file_response, response},
};
use std::sync::Arc;

#[tokio::test]
async fn unified_hf_entry_pins_discovers_and_streams_parquet_shards() {
    let hub = HttpFixture::new();
    let commit = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let bytes = Arc::new(Bytes::from(
        fixture!("parquet", "ordered-snappy.parquet").await,
    ));
    let file = bytes.clone();
    hub.handle(move |request| {
        let uri=request.uri().to_string();
        assert_eq!(request.headers()["authorization"],"Bearer fixture-only");
        if uri.contains("/revision/") {
            return Ok(response(StatusCode::OK,Bytes::from(format!("{{\"sha\":\"{commit}\"}}"))));
        }
        assert!(uri.contains(commit),"discovery and I/O must use pinned export commit");
        if uri.contains("/tree/") || uri.contains("/paths-info/") {
            return Ok(response(StatusCode::OK,Bytes::from(format!(
                "[{{\"type\":\"file\",\"path\":\"default/train/0000.parquet\",\"size\":{},\"oid\":\"{}\"}}]",file.len(),"b".repeat(40)))));
        }
        assert!(uri.contains("/resolve/"));
        Ok(file_response(&request,(*file).clone()))
    });
    let mut request = SourceRequest::new("hf://datasets/owner/example");
    request.service_options.extend([
        ("endpoint".into(), hub.endpoint().into()),
        ("token".into(), "fixture-only".into()),
        ("config".into(), "default".into()),
        ("split".into(), "train".into()),
    ]);
    request.options.batch_size = std::num::NonZeroUsize::MIN;
    let mut reader = SourceRegistry::default().open(request).await.unwrap();
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.num_rows(), 1);
        rows += batch.num_rows();
    }
    assert!(rows > 0);
    let paths: Vec<_> = hub.requests().into_iter().map(|r| r.path).collect();
    assert!(paths.iter().any(|p| p.contains("/revision/")));
    assert!(paths.iter().any(|p| p.contains("/tree/")));
    assert!(paths.iter().any(|p| p.contains("/resolve/")));
}

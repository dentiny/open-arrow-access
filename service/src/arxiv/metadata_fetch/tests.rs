use super::*;
use open_arrow_access_core::{Bytes, ErrorKind};
use open_arrow_access_storage::{RetryConfig, StorageConfig};
use open_arrow_access_test_support::{
    fixture,
    http::{HttpFixture, response},
};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

#[tokio::test]
async fn exhausted_406_keeps_metadata_failure_retryable_and_uncached() {
    let fixture = HttpFixture::new();
    let record = Bytes::from(fixture!("arxiv-content", "metadata-record.xml").await);
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    fixture.handle(move |_| {
        let attempt = observed.fetch_add(1, Ordering::SeqCst);
        Ok(if attempt < 7 {
            response(http::StatusCode::NOT_ACCEPTABLE, Bytes::new())
        } else {
            response(http::StatusCode::OK, record.clone())
        })
    });
    let connection = Connection::new(fixture.endpoint())
        .unwrap()
        .with_context(fixture.context())
        .with_http_config(StorageConfig {
            retry: Some(RetryConfig {
                max_retries: 6,
                min_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
                factor: 2.0,
                jitter: false,
            }),
            timeout: None,
        })
        .unwrap();
    let resolver = MetadataResolver::new(connection);
    let error = resolver
        .get("2410.00001")
        .await
        .err()
        .expect("retry budget exhausted");
    assert_eq!(calls.load(Ordering::SeqCst), 7);
    assert_eq!(error.http_status(), Some(406));
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    assert!(error.to_string().contains("2410.00001"));
    let successful = resolver.get("2410.00001").await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 8);
    let cached = resolver.get("2410.00001").await.unwrap();
    assert!(Arc::ptr_eq(&successful, &cached));
    assert_eq!(calls.load(Ordering::SeqCst), 8);
}

#[tokio::test]
async fn metadata_406_propagates_through_content_without_skipping_articles() {
    use crate::arxiv::{ArxivContentFormat, ArxivKind, ArxivReadRequest, ArxivService};
    use open_arrow_access_core::AccessorExt;
    use open_arrow_access_layers::{MissingObjectLayer, MissingObjectPolicy};
    use open_arrow_access_test_support::MemoryAccessor;

    let fixture = HttpFixture::new();
    fixture.handle(|_| Ok(response(http::StatusCode::NOT_ACCEPTABLE, Bytes::new())));
    let connection = Connection::new(fixture.endpoint())
        .unwrap()
        .with_context(fixture.context());
    let archive = fixture!("arxiv-content", "latex-two.tar").await;
    let skipped = Arc::new(AtomicUsize::new(0));
    let observed = skipped.clone();
    let records = ArxivService::new(MemoryAccessor::new(&archive, 64))
        .with_metadata_connection(connection)
        .records()
        .content(ArxivContentFormat::Latex)
        .layer(MissingObjectLayer::new(
            MissingObjectPolicy::Skip,
            move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        ));
    let mut reader = records
        .open(ArxivReadRequest::new(ArxivKind::Source).with_shard("src/example.tar"))
        .await
        .unwrap();
    let error = reader
        .next()
        .await
        .expect_err("metadata failure must reach the caller");
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    assert_eq!(error.http_status(), Some(406));
    assert!(error.to_string().contains("2410.00001"));
    assert_eq!(skipped.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn explicit_server_delay_does_not_become_an_early_job_retry() {
    let fixture = HttpFixture::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    fixture.handle(move |_| {
        observed.fetch_add(1, Ordering::SeqCst);
        let mut result = response(http::StatusCode::NOT_ACCEPTABLE, Bytes::new());
        result.headers_mut().insert(
            http::header::RETRY_AFTER,
            http::HeaderValue::from_static("3600"),
        );
        Ok(result)
    });
    let connection = Connection::new(fixture.endpoint())
        .unwrap()
        .with_context(fixture.context())
        .with_http_config(StorageConfig {
            retry: Some(RetryConfig {
                max_retries: 6,
                min_delay: Duration::from_millis(1),
                max_delay: Duration::from_secs(600),
                factor: 2.0,
                jitter: false,
            }),
            timeout: None,
        })
        .unwrap();
    let error = MetadataResolver::new(connection)
        .get("2410.00001")
        .await
        .err()
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert_eq!(error.http_status(), Some(406));
    assert_eq!(error.retry_after(), Some("3600"));
    assert!(error.to_string().contains("2410.00001"));
}

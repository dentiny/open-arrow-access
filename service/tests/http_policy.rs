use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use futures::stream;
use http::{Request, Response, StatusCode};
use open_arrow_access_core::ErrorKind;
use open_arrow_access_service::common::Connection;
use open_arrow_access_storage::{RetryConfig, StorageConfig, TimeoutConfig};
use open_arrow_access_test_support::http::{HttpFixture, response};
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter, OperationContext};
use tokio::time::Instant;

mod providers;
use providers::connection;

fn retry_policy() -> StorageConfig {
    StorageConfig {
        retry: Some(RetryConfig {
            max_retries: 2,
            min_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(10),
            jitter: false,
            ..Default::default()
        }),
        timeout: None,
    }
}

#[tokio::test(start_paused = true)]
async fn retry_after_waits_then_succeeds() {
    let fixture = HttpFixture::new();
    let attempts = AtomicUsize::new(0);
    fixture.handle(move |_| {
        let mut res = response(
            if attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                StatusCode::TOO_MANY_REQUESTS
            } else {
                StatusCode::OK
            },
            "{}",
        );
        res.headers_mut()
            .insert("retry-after", "4".parse().unwrap());
        Ok(res)
    });
    let client = connection(&fixture)
        .with_http_config(retry_policy())
        .unwrap();
    let start = Instant::now();
    client.json("page", &[]).await.unwrap();
    assert_eq!(start.elapsed(), Duration::from_secs(4));
    assert_eq!(fixture.requests().len(), 2);
}

#[tokio::test(start_paused = true)]
async fn retries_stop_and_preserve_the_final_response() {
    for (status, retry_after, attempts) in [
        (StatusCode::SERVICE_UNAVAILABLE, "invalid", 3),
        (StatusCode::TOO_MANY_REQUESTS, "3600", 1),
        (StatusCode::NOT_FOUND, "0", 1),
    ] {
        let fixture = HttpFixture::new();
        fixture.handle(move |_| {
            let mut res = response(status, "{}");
            res.headers_mut()
                .insert("retry-after", retry_after.parse().unwrap());
            Ok(res)
        });
        let error = connection(&fixture)
            .with_http_config(retry_policy())
            .unwrap()
            .json("page", &[])
            .await
            .unwrap_err();
        assert_eq!(fixture.requests().len(), attempts);
        assert_eq!(error.http_status(), Some(status.as_u16()));
        assert_eq!(error.retry_after(), Some(retry_after));
    }
}

#[derive(Debug)]
struct HangingTransport {
    before_headers: bool,
}

impl HttpTransport for HangingTransport {
    async fn fetch(&self, _: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        if self.before_headers {
            std::future::pending().await
        } else {
            Ok(Response::new(HttpBody::new(stream::pending(), None)))
        }
    }
}

#[tokio::test(start_paused = true)]
async fn headers_and_body_reads_time_out() {
    for before_headers in [true, false] {
        let context = OperationContext::new()
            .with_http_transport(HttpTransporter::new(HangingTransport { before_headers }));
        let client = Connection::new("https://example.invalid")
            .unwrap()
            .with_context(context)
            .with_http_config(StorageConfig {
                retry: None,
                timeout: Some(TimeoutConfig {
                    operation_timeout: Duration::from_secs(5),
                    io_timeout: Duration::from_secs(3),
                }),
            })
            .unwrap();
        let start = Instant::now();
        assert_eq!(
            client.json("page", &[]).await.unwrap_err().kind(),
            ErrorKind::Timeout
        );
        assert_eq!(
            start.elapsed(),
            Duration::from_secs(if before_headers { 5 } else { 3 })
        );
    }
}

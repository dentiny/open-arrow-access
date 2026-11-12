use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
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

fn policy() -> StorageConfig {
    StorageConfig {
        retry: Some(RetryConfig {
            max_retries: 2,
            min_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(10),
            jitter: false,
            ..Default::default()
        }),
        timeout: Some(TimeoutConfig {
            operation_timeout: Duration::from_secs(5),
            io_timeout: Duration::from_secs(3),
        }),
    }
}

#[tokio::test(start_paused = true)]
async fn retry_after_controls_delay_and_preserves_credentials() {
    let fixture = HttpFixture::new();
    let attempts = Arc::new(AtomicUsize::new(0));
    let count = attempts.clone();
    fixture.handle(move |request| {
        assert_eq!(request.headers()["x-key"], "secret");
        assert_eq!(request.uri().query(), Some("api_key=secret"));
        let mut res = response(
            if count.fetch_add(1, Ordering::SeqCst) == 0 {
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
        .with_http_config(policy())
        .unwrap()
        .with_header("x-key", "secret")
        .unwrap()
        .with_query("api_key", "secret");
    let start = Instant::now();
    client.json("page", &[]).await.unwrap();
    assert_eq!(Instant::now() - start, Duration::from_secs(4));
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn retries_are_bounded_and_keep_final_error_metadata() {
    let fixture = HttpFixture::new();
    fixture.handle(|_| {
        let mut res = response(StatusCode::SERVICE_UNAVAILABLE, "busy");
        res.headers_mut()
            .insert("retry-after", "invalid".parse().unwrap());
        Ok(res)
    });
    let start = Instant::now();
    let error = connection(&fixture)
        .with_http_config(policy())
        .unwrap()
        .json("page", &[])
        .await
        .unwrap_err();
    assert_eq!(fixture.requests().len(), 3);
    assert_eq!(Instant::now() - start, Duration::from_secs(3));
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    assert_eq!(error.http_status(), Some(503));
    assert_eq!(error.retry_after(), Some("invalid"));
}

#[tokio::test(start_paused = true)]
async fn defaults_zero_retries_and_permanent_errors_send_once() {
    for (config, status) in [
        (StorageConfig::default(), StatusCode::SERVICE_UNAVAILABLE),
        (
            StorageConfig {
                retry: Some(RetryConfig {
                    max_retries: 0,
                    ..Default::default()
                }),
                timeout: None,
            },
            StatusCode::TOO_MANY_REQUESTS,
        ),
        (policy(), StatusCode::NOT_FOUND),
        (policy(), StatusCode::UNAUTHORIZED),
        (policy(), StatusCode::FORBIDDEN),
    ] {
        let fixture = HttpFixture::new();
        fixture.respond("/page", status, "{}");
        let start = Instant::now();
        let error = connection(&fixture)
            .with_http_config(config)
            .unwrap()
            .json("page", &[])
            .await
            .unwrap_err();
        assert_eq!(fixture.requests().len(), 1);
        assert_eq!(error.http_status(), Some(status.as_u16()));
        assert_eq!(start.elapsed(), Duration::ZERO);
    }
}

#[tokio::test(start_paused = true)]
async fn excessive_retry_after_returns_instead_of_retrying_early() {
    let fixture = HttpFixture::new();
    fixture.handle(|_| {
        let mut res = response(StatusCode::TOO_MANY_REQUESTS, "{}");
        res.headers_mut()
            .insert("retry-after", "3600".parse().unwrap());
        Ok(res)
    });
    let start = Instant::now();
    let error = connection(&fixture)
        .with_http_config(policy())
        .unwrap()
        .json("page", &[])
        .await
        .unwrap_err();
    assert_eq!(error.retry_after(), Some("3600"));
    assert_eq!(fixture.requests().len(), 1);
    assert_eq!(start.elapsed(), Duration::ZERO);
}

#[derive(Debug)]
struct HangingTransport {
    headers: bool,
    attempts: Arc<AtomicUsize>,
}
impl HttpTransport for HangingTransport {
    async fn fetch(&self, _: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        if self.headers {
            std::future::pending().await
        } else {
            Ok(Response::new(HttpBody::new(stream::pending(), None)))
        }
    }
}

#[tokio::test(start_paused = true)]
async fn header_timeouts_retry_but_body_timeouts_never_replay() {
    for headers in [true, false] {
        let attempts = Arc::new(AtomicUsize::new(0));
        let context =
            OperationContext::new().with_http_transport(HttpTransporter::new(HangingTransport {
                headers,
                attempts: attempts.clone(),
            }));
        // Both builder orders retain the policy.
        let client = if headers {
            Connection::new("https://example.invalid")
                .unwrap()
                .with_http_config(policy())
                .unwrap()
                .with_context(context)
        } else {
            Connection::new("https://example.invalid")
                .unwrap()
                .with_context(context)
                .with_http_config(policy())
                .unwrap()
        };
        let start = Instant::now();
        let error = client.json("page", &[]).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Timeout);
        assert_eq!(attempts.load(Ordering::SeqCst), if headers { 3 } else { 1 });
        assert_eq!(
            start.elapsed(),
            Duration::from_secs(if headers { 18 } else { 3 })
        );
    }
}

#[tokio::test]
async fn invalid_policy_is_rejected_before_io() {
    let fixture = HttpFixture::new();
    let mut config = policy();
    config.retry.as_mut().unwrap().factor = f32::NAN;
    assert!(connection(&fixture).with_http_config(config).is_err());
    let mut config = policy();
    config.timeout.as_mut().unwrap().io_timeout = Duration::ZERO;
    assert!(connection(&fixture).with_http_config(config).is_err());
    assert!(fixture.requests().is_empty());
}

#[tokio::test(start_paused = true)]
async fn retries_keep_cross_origin_redirect_credentials_scoped() {
    let provider = HttpFixture::new();
    let download = HttpFixture::new();
    let target = format!("{}/page?sig=a%2Bb", download.endpoint());
    provider.handle(move |request| {
        assert_eq!(request.headers()["x-key"], "secret");
        assert_eq!(request.uri().query(), Some("api_key=secret"));
        let mut res = response(StatusCode::FOUND, "");
        res.headers_mut()
            .insert("location", target.parse().unwrap());
        Ok(res)
    });
    let attempts = Arc::new(AtomicUsize::new(0));
    let count = attempts.clone();
    download.handle(move |request| {
        assert!(!request.headers().contains_key("x-key"));
        assert_eq!(request.uri().query(), Some("sig=a%2Bb"));
        Ok(response(
            if count.fetch_add(1, Ordering::SeqCst) == 0 {
                StatusCode::TOO_MANY_REQUESTS
            } else {
                StatusCode::OK
            },
            "{}",
        ))
    });
    connection(&provider)
        .with_http_config(policy())
        .unwrap()
        .with_header("x-key", "secret")
        .unwrap()
        .with_query("api_key", "secret")
        .json("start", &[])
        .await
        .unwrap();
    assert_eq!(provider.requests().len(), 2);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[cfg(feature = "zenodo")]
#[tokio::test(start_paused = true)]
async fn managed_file_provider_applies_policy_to_manifest_and_lazy_storage() {
    use open_arrow_access_core::{Accessor, read_all};
    use open_arrow_access_service::zenodo::ZenodoService;
    let fixture = HttpFixture::new();
    let manifest_attempts = Arc::new(AtomicUsize::new(0));
    let stat_attempts = Arc::new(AtomicUsize::new(0));
    let manifest = manifest_attempts.clone();
    let stat = stat_attempts.clone();
    fixture.handle(move |request| {
        if request.uri().path() == "/api/records/1" {
            return Ok(response(
                if manifest.fetch_add(1, Ordering::SeqCst) == 0 {
                    StatusCode::SERVICE_UNAVAILABLE
                } else {
                    StatusCode::OK
                },
                r#"{"id":1,"files":[{"key":"test","size":4,"checksum":"md5:unused"}]}"#,
            ));
        }
        assert_eq!(request.uri().path(), "/api/records/1/files/test/content");
        if request.method() == http::Method::HEAD {
            let mut res = response(
                if stat.fetch_add(1, Ordering::SeqCst) == 0 {
                    StatusCode::SERVICE_UNAVAILABLE
                } else {
                    StatusCode::OK
                },
                "",
            );
            res.headers_mut()
                .insert("content-length", "4".parse().unwrap());
            Ok(res)
        } else {
            Ok(response(StatusCode::OK, "data"))
        }
    });
    let service = ZenodoService::with_endpoint(fixture.endpoint())
        .unwrap()
        .with_http_config(policy())
        .unwrap();
    let files = service
        .record(std::num::NonZeroU64::new(1).unwrap())
        .await
        .unwrap();
    assert_eq!(fixture.requests().len(), 2, "no eager file requests");
    let reader = files.open("test".into()).await.unwrap();
    assert_eq!(read_all(reader, "test").await.unwrap(), b"data");
    assert_eq!(manifest_attempts.load(Ordering::SeqCst), 2);
    assert_eq!(stat_attempts.load(Ordering::SeqCst), 2);
}

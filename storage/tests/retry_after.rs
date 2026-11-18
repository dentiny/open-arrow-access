use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use http::{Request, Response, StatusCode};
use open_arrow_access_storage::{RetryConfig, StorageConfig, TimeoutConfig};
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter, Operator};

const BODY: &[u8] = b"payload";

/// Throttles the first request with `retry_after`, then serves `BODY`.
struct Throttled {
    retry_after: &'static str,
    requests: Arc<AtomicUsize>,
}

impl HttpTransport for Throttled {
    async fn fetch(&self, _: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        let response = if self.requests.fetch_add(1, Ordering::SeqCst) == 0 {
            Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .header(http::header::RETRY_AFTER, self.retry_after)
                .body(HttpBody::new(futures::stream::empty(), Some(0)))
        } else {
            Response::builder()
                .status(StatusCode::OK)
                .header(http::header::CONTENT_LENGTH, BODY.len())
                .body(HttpBody::new(
                    futures::stream::iter([Ok(Buffer::from(BODY))]),
                    Some(BODY.len() as u64),
                ))
        };
        Ok(response.unwrap())
    }
}

fn operator(retry_after: &'static str, requests: Arc<AtomicUsize>) -> Operator {
    let operator =
        Operator::new(opendal_service_http::Http::default().endpoint("http://retry-after.invalid"))
            .unwrap();
    let context = operator
        .base_context()
        .with_http_transport(HttpTransporter::new(Throttled {
            retry_after,
            requests,
        }));
    StorageConfig {
        retry: Some(RetryConfig {
            max_retries: 1,
            min_delay: Duration::from_millis(1),
            max_delay: Duration::from_secs(5),
            factor: 2.0,
            jitter: false,
        }),
        // Shorter than the server delay, which must not count against an attempt.
        timeout: Some(TimeoutConfig {
            operation_timeout: Duration::from_millis(500),
            io_timeout: Duration::from_millis(500),
        }),
    }
    .apply(operator.with_context(context))
    .unwrap()
}

#[tokio::test]
async fn retries_wait_for_server_delay_outside_attempt_timeouts() {
    let requests = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let data = operator("1", requests.clone()).read("blob").await.unwrap();
    assert_eq!(data.to_vec(), BODY);
    assert!(requests.load(Ordering::SeqCst) >= 2);
    assert!(started.elapsed() >= Duration::from_secs(1));
}

#[tokio::test]
async fn server_delay_above_max_delay_is_not_retried() {
    let requests = Arc::new(AtomicUsize::new(0));
    assert!(operator("60", requests.clone()).read("blob").await.is_err());
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}

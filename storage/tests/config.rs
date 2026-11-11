use futures::{future, stream};
use http::{Method, Request, Response, StatusCode};
use open_arrow_access_core::Accessor;
use open_arrow_access_storage::{OpenDalAccessor, RetryConfig, StorageConfig, TimeoutConfig};
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter, OperationContext, Operator};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct Transport {
    calls: Arc<AtomicUsize>,
    failures: usize,
    stall_first: bool,
    stall_body: bool,
}
impl HttpTransport for Transport {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.stall_first && call == 0 {
            future::pending::<()>().await;
        }
        if call < self.failures {
            return Ok(Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .body(HttpBody::new(stream::empty(), Some(0)))
                .unwrap());
        }
        let body = if request.method() == Method::HEAD {
            HttpBody::new(stream::empty(), Some(0))
        } else if self.stall_body {
            HttpBody::new(stream::pending(), Some(3))
        } else {
            HttpBody::new(stream::iter([Ok(Buffer::from("abc"))]), Some(3))
        };
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-length", 3)
            .body(body)
            .unwrap())
    }
}
fn operator(transport: Transport) -> Operator {
    Operator::new(opendal_service_http::Http::default().endpoint("https://storage.test"))
        .unwrap()
        .with_context(OperationContext::new().with_http_transport(HttpTransporter::new(transport)))
}
fn retry() -> RetryConfig {
    RetryConfig {
        max_retries: 2,
        min_delay: Duration::ZERO,
        max_delay: Duration::ZERO,
        ..Default::default()
    }
}
fn timeout() -> TimeoutConfig {
    TimeoutConfig {
        operation_timeout: Duration::from_millis(10),
        io_timeout: Duration::from_millis(10),
    }
}

#[tokio::test]
async fn retries_are_opt_in_and_obey_the_configured_limit() {
    for (retry, failures, expected_calls, success) in [
        (None, 2, 1, false),
        (Some(retry()), 2, 3, true),
        (Some(retry()), 3, 3, false),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let accessor = OpenDalAccessor::try_new(
            operator(Transport {
                calls: calls.clone(),
                failures,
                stall_first: false,
                stall_body: false,
            }),
            StorageConfig {
                retry,
                timeout: None,
            },
        )
        .unwrap();
        assert_eq!(accessor.open("file".into()).await.is_ok(), success);
        assert_eq!(calls.load(Ordering::SeqCst), expected_calls);
    }
}

#[tokio::test]
async fn timed_out_attempt_can_be_retried_and_body_reads_have_a_timeout() {
    let calls = Arc::new(AtomicUsize::new(0));
    let accessor = OpenDalAccessor::try_new(
        operator(Transport {
            calls: calls.clone(),
            failures: 0,
            stall_first: true,
            stall_body: false,
        }),
        StorageConfig {
            retry: Some(retry()),
            timeout: Some(timeout()),
        },
    )
    .unwrap();
    let mut reader = tokio::time::timeout(Duration::from_secs(1), accessor.open("file".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), b"abc");
    let accessor = OpenDalAccessor::try_new(
        operator(Transport {
            calls: Arc::new(AtomicUsize::new(0)),
            failures: 0,
            stall_first: false,
            stall_body: true,
        }),
        StorageConfig {
            retry: None,
            timeout: Some(timeout()),
        },
    )
    .unwrap();
    let mut reader = accessor.open("file".into()).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), reader.next())
            .await
            .unwrap()
            .is_err()
    );
    assert!(reader.next().await.unwrap().is_none());
}

#[test]
fn invalid_settings_are_rejected_during_construction() {
    use open_arrow_access_core::ErrorKind;
    for config in [
        StorageConfig {
            retry: Some(RetryConfig {
                factor: f32::NAN,
                ..Default::default()
            }),
            timeout: None,
        },
        StorageConfig {
            retry: Some(RetryConfig {
                min_delay: Duration::from_secs(2),
                max_delay: Duration::from_secs(1),
                ..Default::default()
            }),
            timeout: None,
        },
        StorageConfig {
            retry: None,
            timeout: Some(TimeoutConfig {
                io_timeout: Duration::ZERO,
                ..Default::default()
            }),
        },
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let result = OpenDalAccessor::try_new(
            operator(Transport {
                calls: calls.clone(),
                failures: 0,
                stall_first: false,
                stall_body: false,
            }),
            config,
        );
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidInput);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

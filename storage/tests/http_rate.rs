use http::{Request, Response, StatusCode};
use open_arrow_access_storage::HttpReadRateLimitLayer;
use opendal::{
    Buffer, HttpBody, HttpTransport, HttpTransporter, Operator,
    layers::{RetryLayer, TimeoutLayer},
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Clone, Default)]
struct Server {
    starts: Arc<Mutex<Vec<Instant>>>,
    attempts: Arc<AtomicUsize>,
    forbidden: bool,
}
impl HttpTransport for Server {
    async fn fetch(&self, _: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        self.starts.lock().unwrap().push(Instant::now());
        if self.forbidden && self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Ok(Response::builder()
                .status(StatusCode::FORBIDDEN)
                .header(http::header::RETRY_AFTER, "1")
                .body(HttpBody::new(futures::stream::empty(), Some(0)))
                .unwrap());
        }
        Ok(Response::builder()
            .header(http::header::CONTENT_LENGTH, 1)
            .body(HttpBody::new(
                futures::stream::iter([Ok(Buffer::from("x"))]),
                Some(1),
            ))
            .unwrap())
    }
}
fn operator(server: Server, limit: HttpReadRateLimitLayer) -> Operator {
    let operator =
        Operator::new(opendal_service_http::Http::default().endpoint("http://rate.invalid"))
            .unwrap();
    let context = operator
        .base_context()
        .with_http_transport(HttpTransporter::new(server));
    operator
        .with_context(context)
        .layer(
            TimeoutLayer::new()
                .with_timeout(Duration::from_millis(100))
                .with_io_timeout(Duration::from_millis(100)),
        )
        .layer(limit)
        .layer(
            RetryLayer::new()
                .with_max_times(1)
                .with_min_delay(Duration::from_millis(1))
                .with_max_delay(Duration::from_millis(1)),
        )
}
#[tokio::test]
async fn cloned_layers_pace_requests_across_independent_operators() {
    let server = Server::default();
    let interval = Duration::from_millis(150);
    let rate = HttpReadRateLimitLayer::new(interval).unwrap();
    let a = operator(server.clone(), rate.clone());
    let b = operator(server.clone(), rate);
    let (a, b) = tokio::join!(a.read("a"), b.read("b"));
    assert_eq!(a.unwrap().to_vec(), b"x");
    assert_eq!(b.unwrap().to_vec(), b"x");
    let starts = server.starts.lock().unwrap();
    assert_eq!(starts.len(), 2);
    assert!(starts[1].duration_since(starts[0]) >= interval);
}
#[tokio::test]
async fn forbidden_cooldown_is_opt_in_and_precedes_attempt_timeouts() {
    for retry_forbidden in [false, true] {
        let server = Server {
            forbidden: true,
            ..Default::default()
        };
        let rate = HttpReadRateLimitLayer::new(Duration::from_millis(1)).unwrap();
        let rate = if retry_forbidden {
            rate.with_forbidden_throttling()
        } else {
            rate
        };
        let started = Instant::now();
        let result = operator(server.clone(), rate).read("body").await;
        if retry_forbidden {
            assert_eq!(result.unwrap().to_vec(), b"x");
            assert!(started.elapsed() >= Duration::from_secs(1));
            assert_eq!(server.starts.lock().unwrap().len(), 2);
        } else {
            assert!(result.is_err());
            assert_eq!(server.starts.lock().unwrap().len(), 1);
        }
    }
}
#[tokio::test]
async fn cancelled_waiter_does_not_hold_the_shared_request_gate() {
    let server = Server::default();
    let rate = HttpReadRateLimitLayer::new(Duration::from_millis(40)).unwrap();
    let first = operator(server.clone(), rate.clone());
    first.read("first").await.unwrap();
    let second = operator(server.clone(), rate.clone());
    let waiter = tokio::spawn(async move { second.read("cancelled").await });
    tokio::task::yield_now().await;
    waiter.abort();
    let _ = waiter.await;
    let third = operator(server.clone(), rate);
    tokio::time::timeout(Duration::from_secs(1), third.read("third"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(server.starts.lock().unwrap().len(), 2);
}
#[test]
fn zero_request_interval_is_rejected() {
    assert!(HttpReadRateLimitLayer::new(Duration::ZERO).is_err());
}

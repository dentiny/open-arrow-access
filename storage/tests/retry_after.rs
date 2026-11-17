use futures::{poll, stream};
use http::{Request, Response};
use open_arrow_access_storage::{
    HttpConcurrencyLimitLayer, RetryConfig, StorageConfig, TimeoutConfig,
};
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter, Operator};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};
use tokio::time::{Instant, advance};

#[derive(Clone)]
enum Reply {
    Status(u16, Option<String>),
    Temporary,
    Slow,
}
#[derive(Clone)]
struct Fixture {
    replies: Arc<Mutex<VecDeque<Reply>>>,
    attempts: Arc<Mutex<Vec<Instant>>>,
}
impl Fixture {
    fn new(replies: impl IntoIterator<Item = Reply>) -> Self {
        Self {
            replies: Arc::new(Mutex::new(replies.into_iter().collect())),
            attempts: Default::default(),
        }
    }
    fn count(&self) -> usize {
        self.attempts.lock().unwrap().len()
    }
}
impl HttpTransport for Fixture {
    async fn fetch(&self, _: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        self.attempts.lock().unwrap().push(Instant::now());
        let reply = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Reply::Status(200, None));
        let (status, header) = match reply {
            Reply::Status(status, header) => (status, header),
            Reply::Temporary => {
                return Err(
                    opendal::Error::new(opendal::ErrorKind::Unexpected, "network failure")
                        .set_temporary(),
                );
            }
            Reply::Slow => {
                tokio::time::sleep(Duration::from_secs(20)).await;
                (200, None)
            }
        };
        let mut response = Response::builder()
            .status(status)
            .header("content-length", "4");
        if let Some(header) = header {
            response = response.header("retry-after", header);
        }
        Ok(response
            .body(if status == 200 {
                HttpBody::new(stream::iter([Ok(Buffer::from("data"))]), Some(4))
            } else {
                // The policy must drop failed responses rather than read this body.
                HttpBody::new(stream::pending(), None)
            })
            .unwrap())
    }
}
fn status(code: u16, header: &str) -> Reply {
    Reply::Status(code, Some(header.to_owned()))
}
fn config(retries: usize) -> StorageConfig {
    StorageConfig {
        retry: Some(RetryConfig {
            max_retries: retries,
            min_delay: Duration::from_millis(10),
            max_delay: Duration::from_secs(10),
            factor: 2.0,
            jitter: false,
        }),
        timeout: Some(TimeoutConfig {
            operation_timeout: Duration::from_millis(100),
            io_timeout: Duration::from_millis(100),
        }),
    }
}
fn operator(fixture: &Fixture, config: StorageConfig) -> Operator {
    let operator =
        Operator::new(opendal_service_http::Http::default().endpoint("https://fixture.invalid"))
            .unwrap();
    let operator = config.apply(operator).unwrap();
    // Override the transport AFTER applying policies, as provider adapters do.
    let limit = HttpConcurrencyLimitLayer::new(1).unwrap();
    let ctx = operator
        .base_context()
        .with_http_transport(limit.wrap(HttpTransporter::new(fixture.clone())));
    operator.with_context(ctx)
}

#[tokio::test(start_paused = true)]
async fn rate_limit_wait_exceeds_attempt_timeout_and_releases_http_permit() {
    for code in [429, 503] {
        let fixture = Fixture::new([status(code, "2")]);
        let operator = operator(&fixture, config(1));
        let mut first = Box::pin(operator.stat("first"));
        assert!(poll!(&mut first).is_pending());
        assert_eq!(fixture.count(), 1);
        // With one permit, this independent request can finish during backoff.
        operator.stat("other").await.unwrap();
        assert_eq!(fixture.count(), 2);
        advance(Duration::from_millis(1999)).await;
        assert!(poll!(&mut first).is_pending());
        assert_eq!(fixture.count(), 2);
        advance(Duration::from_millis(1)).await;
        first.await.unwrap();
        assert_eq!(fixture.count(), 3);
    }
}

#[tokio::test(start_paused = true)]
async fn rate_limits_apply_to_reader_open_and_preserve_bytes() {
    let fixture = Fixture::new([status(429, "2")]);
    let operator = operator(&fixture, config(1));
    let reader = operator.reader("blob").await.unwrap();
    let mut read = Box::pin(reader.read(..));
    assert!(poll!(&mut read).is_pending());
    advance(Duration::from_secs(2)).await;
    assert_eq!(read.await.unwrap().to_vec(), b"data");
    assert_eq!(fixture.count(), 2);
}

#[tokio::test(start_paused = true)]
async fn http_date_header_is_honored() {
    let date = httpdate::fmt_http_date(SystemTime::now() + Duration::from_secs(10));
    let fixture = Fixture::new([status(503, &date)]);
    let operator = operator(&fixture, config(1));
    let mut stat = Box::pin(operator.stat("blob"));
    assert!(poll!(&mut stat).is_pending());
    advance(Duration::from_secs(8)).await;
    assert!(poll!(&mut stat).is_pending());
    assert_eq!(fixture.count(), 1);
    advance(Duration::from_secs(2)).await;
    stat.await.unwrap();
    assert_eq!(fixture.count(), 2);
}

#[tokio::test(start_paused = true)]
async fn excessive_server_delay_stops_instead_of_retrying_early() {
    for value in ["11", "9999999999999999999999999999999999999"] {
        let fixture = Fixture::new([status(429, value)]);
        let operator = operator(&fixture, config(3));
        let error = operator.stat("blob").await.unwrap_err();
        assert!(!error.is_temporary());
        assert!(format!("{error:?}").contains("429"));
        assert!(format!("{error:?}").contains(value));
        assert_eq!(fixture.count(), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn exhausted_budget_never_gets_revived_by_a_header() {
    for retries in [0, 1, 2] {
        let fixture = Fixture::new(std::iter::repeat_n(status(503, "1"), 5));
        let operator = operator(&fixture, config(retries));
        assert!(operator.stat("blob").await.is_err());
        assert_eq!(fixture.count(), retries + 1);
    }
}

#[tokio::test(start_paused = true)]
async fn missing_invalid_and_past_headers_use_normal_backoff() {
    for value in [
        None,
        Some("bad"),
        Some("-1"),
        Some("0"),
        Some("Sun, 06 Nov 1994 08:49:37 GMT"),
    ] {
        let fixture = Fixture::new([Reply::Status(503, value.map(str::to_owned))]);
        let operator = operator(&fixture, config(1));
        let start = Instant::now();
        operator.stat("blob").await.unwrap();
        assert_eq!(Instant::now() - start, Duration::from_millis(10));
        assert_eq!(fixture.count(), 2);
    }
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_backoff_never_replays() {
    let fixture = Fixture::new([status(429, "2")]);
    let operator = operator(&fixture, config(3));
    let mut stat = Box::pin(operator.stat("blob"));
    assert!(poll!(&mut stat).is_pending());
    drop(stat);
    advance(Duration::from_secs(60)).await;
    assert_eq!(fixture.count(), 1);
}

#[tokio::test(start_paused = true)]
async fn temporary_transport_errors_and_attempt_timeouts_still_retry() {
    for reply in [Reply::Temporary, Reply::Slow] {
        let fixture = Fixture::new([reply]);
        let operator = operator(&fixture, config(1));
        operator.stat("blob").await.unwrap();
        assert_eq!(fixture.count(), 2);
    }
}

#[tokio::test(start_paused = true)]
async fn permanent_status_is_not_retried() {
    // An empty error body lets the backend finish its normal status parsing.
    struct Missing(Arc<Mutex<usize>>);
    impl HttpTransport for Missing {
        async fn fetch(&self, _: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
            *self.0.lock().unwrap() += 1;
            Ok(Response::builder()
                .status(404)
                .body(HttpBody::new(stream::empty(), Some(0)))
                .unwrap())
        }
    }
    let calls = Arc::new(Mutex::new(0));
    let operator =
        Operator::new(opendal_service_http::Http::default().endpoint("https://fixture.invalid"))
            .unwrap();
    let operator = config(3).apply(operator).unwrap();
    let ctx = operator
        .base_context()
        .with_http_transport(HttpTransporter::new(Missing(calls.clone())));
    assert_eq!(
        operator
            .with_context(ctx)
            .stat("blob")
            .await
            .unwrap_err()
            .kind(),
        opendal::ErrorKind::NotFound
    );
    assert_eq!(*calls.lock().unwrap(), 1);
}

#[tokio::test(start_paused = true)]
async fn interrupted_stream_resumes_at_the_delivered_offset_after_rate_limit() {
    use futures::TryStreamExt;
    #[derive(Clone, Default)]
    struct Resumed(Arc<Mutex<Vec<Option<String>>>>);
    impl HttpTransport for Resumed {
        async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
            let mut ranges = self.0.lock().unwrap();
            ranges.push(
                request
                    .headers()
                    .get("range")
                    .map(|value| value.to_str().unwrap().to_owned()),
            );
            Ok(match ranges.len() {
                1 => Response::builder()
                    .status(200)
                    .header("content-length", "4")
                    .body(HttpBody::new(
                        stream::iter([
                            Ok(Buffer::from("da")),
                            Err(opendal::Error::new(
                                opendal::ErrorKind::Unexpected,
                                "broken connection",
                            )
                            .set_temporary()),
                        ]),
                        Some(4),
                    ))
                    .unwrap(),
                2 => Response::builder()
                    .status(503)
                    .header("retry-after", "2")
                    .body(HttpBody::new(stream::empty(), Some(0)))
                    .unwrap(),
                _ => Response::builder()
                    .status(206)
                    .header("content-length", "2")
                    .header("content-range", "bytes 2-3/4")
                    .body(HttpBody::new(
                        stream::iter([Ok(Buffer::from("ta"))]),
                        Some(2),
                    ))
                    .unwrap(),
            })
        }
    }
    let transport = Resumed::default();
    let operator =
        Operator::new(opendal_service_http::Http::default().endpoint("https://fixture.invalid"))
            .unwrap();
    let operator = config(2).apply(operator).unwrap();
    let ctx = operator
        .base_context()
        .with_http_transport(HttpTransporter::new(transport.clone()));
    let reader = operator.with_context(ctx).reader("blob").await.unwrap();
    let mut chunks = reader.into_stream(..).await.unwrap();
    assert_eq!(chunks.try_next().await.unwrap().unwrap().to_vec(), b"da");
    let start = Instant::now();
    assert_eq!(chunks.try_next().await.unwrap().unwrap().to_vec(), b"ta");
    assert!(Instant::now() - start >= Duration::from_secs(2));
    assert!(chunks.try_next().await.unwrap().is_none());
    let ranges = transport.0.lock().unwrap();
    assert_eq!(ranges.len(), 3);
    assert_eq!(ranges[1].as_deref(), Some("bytes=2-"));
    assert_eq!(ranges[2].as_deref(), Some("bytes=2-"));
}

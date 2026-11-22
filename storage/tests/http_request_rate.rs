use std::{sync::Arc, time::Duration};

use futures::{FutureExt, pin_mut, poll};
use http::{Request, Response};
use open_arrow_access_storage::HttpRequestRateLimitLayer;
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter, Operator};
use tokio::time::Instant;

fn limit() -> HttpRequestRateLimitLayer {
    HttpRequestRateLimitLayer::new(Duration::from_secs(3)).unwrap()
}

#[tokio::test(start_paused = true)]
async fn requests_wait_for_both_spacing_and_previous_processing() {
    let gate = limit();
    let start = Instant::now();
    let first = gate.acquire().await;
    let clone = gate.clone();
    let second = clone.acquire().fuse();
    pin_mut!(second);
    assert!(poll!(&mut second).is_pending());
    tokio::time::advance(Duration::from_secs(4)).await;
    assert!(poll!(&mut second).is_pending());
    drop(first);
    let second = second.await;
    assert_eq!(Instant::now() - start, Duration::from_secs(4));
    drop(second);
    let _third = gate.acquire().await;
    assert_eq!(Instant::now() - start, Duration::from_secs(7));
}

#[tokio::test(start_paused = true)]
async fn cancelled_waiter_does_not_reserve_an_extra_interval() {
    let gate = limit();
    drop(gate.acquire().await);
    {
        let waiting = gate.acquire();
        pin_mut!(waiting);
        assert!(poll!(&mut waiting).is_pending());
    }
    let start = Instant::now();
    let _permit = gate.acquire().await;
    assert_eq!(Instant::now() - start, Duration::from_secs(3));
}

#[tokio::test(start_paused = true)]
async fn independent_layers_do_not_share_admission() {
    let first = limit();
    let second = limit();
    let _first = first.acquire().await;
    assert!(poll!(Box::pin(second.acquire())).is_ready());
}

#[derive(Clone, Copy, Debug)]
enum Outcome {
    Body,
    BodyError,
    FetchError,
    PendingFetch,
}

#[derive(Clone, Debug)]
struct Server {
    starts: Arc<std::sync::Mutex<Vec<Instant>>>,
    outcome: Outcome,
}

impl HttpTransport for Server {
    async fn fetch(&self, _: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        self.starts.lock().unwrap().push(Instant::now());
        let body = match self.outcome {
            Outcome::Body => HttpBody::new(futures::stream::iter([Ok(Buffer::from("x"))]), Some(1)),
            Outcome::BodyError => HttpBody::new(
                futures::stream::iter([Err(opendal::Error::new(
                    opendal::ErrorKind::Unexpected,
                    "body failed",
                ))]),
                None,
            ),
            Outcome::FetchError => {
                return Err(opendal::Error::new(
                    opendal::ErrorKind::Unexpected,
                    "fetch failed",
                ));
            }
            Outcome::PendingFetch => return futures::future::pending().await,
        };
        Ok(Response::builder()
            .header(http::header::CONTENT_LENGTH, "1")
            .body(body)
            .unwrap())
    }
}

fn server(outcome: Outcome) -> Server {
    Server {
        starts: Arc::default(),
        outcome,
    }
}

fn request() -> Request<Buffer> {
    Request::get("http://rate.invalid/body")
        .body(Buffer::new())
        .unwrap()
}

#[rstest::rstest]
#[case::eof(Outcome::Body, true)]
#[case::body_error(Outcome::BodyError, true)]
#[case::body_drop(Outcome::Body, false)]
#[tokio::test(start_paused = true)]
async fn transport_holds_admission_until_body_finishes(
    #[case] outcome: Outcome,
    #[case] consume: bool,
) {
    let gate = limit();
    let transport = gate.wrap(HttpTransporter::new(server(outcome)));
    let mut response = transport.fetch(request()).await.unwrap();
    let next = gate.acquire().fuse();
    pin_mut!(next);
    tokio::time::advance(Duration::from_secs(4)).await;
    assert!(poll!(&mut next).is_pending());
    if consume {
        let result = response.body_mut().to_buffer().await;
        assert_eq!(result.is_err(), matches!(outcome, Outcome::BodyError));
        // EOF/error returns admission even while the response is still alive.
        assert!(poll!(&mut next).is_ready());
    } else {
        drop(response);
        assert!(poll!(&mut next).is_ready());
    }
}

#[rstest::rstest]
#[case::fetch_error(Outcome::FetchError)]
#[case::fetch_cancelled(Outcome::PendingFetch)]
#[tokio::test(start_paused = true)]
async fn failed_or_cancelled_fetch_releases_admission(#[case] outcome: Outcome) {
    let gate = limit();
    let transport = gate.wrap(HttpTransporter::new(server(outcome)));
    {
        let fetch = transport.fetch(request());
        pin_mut!(fetch);
        match outcome {
            Outcome::FetchError => assert!(fetch.await.is_err()),
            Outcome::PendingFetch => assert!(poll!(&mut fetch).is_pending()),
            _ => unreachable!(),
        }
    }
    let start = Instant::now();
    let _permit = gate.acquire().await;
    assert_eq!(Instant::now() - start, Duration::from_secs(3));
}

#[tokio::test(start_paused = true)]
async fn cloned_layers_pace_independent_opendal_operators() {
    let gate = limit();
    let server = server(Outcome::Body);
    let operator = || {
        let builder =
            Operator::new(opendal_service_http::Http::default().endpoint("http://rate.invalid"))
                .unwrap();
        let context = builder
            .base_context()
            .with_http_transport(HttpTransporter::new(server.clone()));
        builder.with_context(context).layer(gate.clone())
    };
    let first = operator();
    let second = operator();
    let (a, b) = tokio::join!(first.read("a"), second.read("b"));
    assert_eq!(a.unwrap().to_vec(), b"x");
    assert_eq!(b.unwrap().to_vec(), b"x");
    let starts = server.starts.lock().unwrap();
    assert_eq!(starts.len(), 2);
    assert_eq!(starts[1] - starts[0], Duration::from_secs(3));
}

#[test]
fn rejects_invalid_intervals() {
    assert!(HttpRequestRateLimitLayer::new(Duration::ZERO).is_err());
    assert!(HttpRequestRateLimitLayer::new(Duration::MAX).is_err());
}

use std::time::Duration;

use futures::{FutureExt, pin_mut, poll};
use http::{Request, Response};
use open_arrow_access_storage::HttpRequestRateLimitLayer;
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter};
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

#[derive(Clone, Copy, Debug)]
enum Outcome {
    Body,
    BodyError,
}

#[derive(Clone, Debug)]
struct Server {
    outcome: Outcome,
}

impl HttpTransport for Server {
    async fn fetch(&self, _: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        let body = match self.outcome {
            Outcome::Body => HttpBody::new(futures::stream::iter([Ok(Buffer::from("x"))]), Some(1)),
            Outcome::BodyError => HttpBody::new(
                futures::stream::iter([Err(opendal::Error::new(
                    opendal::ErrorKind::Unexpected,
                    "body failed",
                ))]),
                None,
            ),
        };
        Ok(Response::builder()
            .header(http::header::CONTENT_LENGTH, "1")
            .body(body)
            .unwrap())
    }
}

fn server(outcome: Outcome) -> Server {
    Server { outcome }
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

use std::{
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};

use futures::StreamExt;
use http::{Method, Request, Response, StatusCode, header};
use open_arrow_access_core::{Accessor, ByteReadOptions, ByteReadRequest, read_all};
use open_arrow_access_storage::{HttpConcurrencyLimitLayer, OpenDalAccessor};
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter, Operator};

#[derive(Clone, Default)]
struct Requests(Arc<Mutex<RequestLog>>, Arc<AtomicUsize>);

type RequestLog = Vec<(Method, Option<String>)>;

struct Server {
    bytes: Vec<u8>,
    declared_length: usize,
    pending: bool,
    requests: Requests,
}

impl HttpTransport for Server {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        let range = request
            .headers()
            .get(header::RANGE)
            .map(|v| v.to_str().unwrap().to_owned());
        self.requests
            .0
            .lock()
            .unwrap()
            .push((request.method().clone(), range.clone()));
        let mut response = Response::builder();
        let (start, end) = if let Some(range) = range {
            let (start, end) = range
                .strip_prefix("bytes=")
                .unwrap()
                .split_once('-')
                .unwrap();
            let start: usize = start.parse().unwrap();
            if start >= self.bytes.len() {
                return Ok(response
                    .status(StatusCode::RANGE_NOT_SATISFIABLE)
                    .header(
                        header::CONTENT_RANGE,
                        format!("bytes */{}", self.declared_length),
                    )
                    .body(HttpBody::new(futures::stream::empty(), Some(0)))
                    .unwrap());
            }
            let end = if end.is_empty() {
                self.bytes.len()
            } else {
                (end.parse::<usize>().unwrap() + 1).min(self.bytes.len())
            };
            response = response.status(StatusCode::PARTIAL_CONTENT).header(
                header::CONTENT_RANGE,
                format!("bytes {start}-{}/{}", end - 1, self.declared_length),
            );
            (start, end)
        } else {
            response = response.status(StatusCode::OK);
            (0, self.bytes.len())
        };
        let length = if request.headers().contains_key(header::RANGE) {
            end - start
        } else {
            self.declared_length
        };
        response = response.header(header::CONTENT_LENGTH, length);
        let body = if request.method() == Method::HEAD {
            HttpBody::new(futures::stream::empty(), Some(0))
        } else if self.pending {
            HttpBody::new(futures::stream::pending(), Some(length as u64))
        } else {
            let completed = self.requests.1.clone();
            let data = (start != end).then(|| Ok(Buffer::from(self.bytes[start..end].to_vec())));
            HttpBody::new(
                futures::stream::iter(data).chain(futures::stream::poll_fn(move |_| {
                    completed.fetch_add(1, Ordering::SeqCst);
                    Poll::Ready(None)
                })),
                Some(length as u64),
            )
        };
        Ok(response.body(body).unwrap())
    }
}

fn storage(bytes: Vec<u8>, declared_length: usize, pending: bool) -> (OpenDalAccessor, Requests) {
    let requests = Requests::default();
    let operator =
        Operator::new(opendal_service_http::Http::default().endpoint("http://sequential.invalid"))
            .unwrap();
    let context = operator
        .base_context()
        .with_http_transport(HttpTransporter::new(Server {
            bytes,
            declared_length,
            pending,
            requests: requests.clone(),
        }));
    (
        OpenDalAccessor::new(operator.with_context(context)),
        requests,
    )
}

#[tokio::test]
async fn default_sequential_reads_empty_small_and_large_objects_with_one_get() {
    for size in [1, 17, 0, 2 * 1024 * 1024 + 3] {
        let bytes: Vec<_> = (0..size).map(|n| (n % 251) as u8).collect();
        let (storage, requests) = storage(bytes.clone(), size, false);
        let storage = storage.with_chunk_size(NonZeroUsize::new(7).unwrap());
        let mut reader = storage.open_sequential("object".into()).await.unwrap();
        assert_eq!(reader.metadata().content_length, Some(size as u64));
        let mut output = Vec::new();
        while let Some(chunk) = reader.next().await.unwrap() {
            assert!(!chunk.is_empty() && chunk.len() <= 7);
            output.extend_from_slice(&chunk);
        }
        assert_eq!(output, bytes);
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(*requests.0.lock().unwrap(), [(Method::GET, None)]);
        assert_eq!(
            requests.1.load(Ordering::SeqCst),
            1,
            "body EOF must be consumed"
        );
    }
}

#[tokio::test]
async fn sequential_readers_reject_ranges_without_waiting_for_another_http_permit() {
    let bytes: Vec<_> = (0..32).collect();
    let (storage, requests) = storage(bytes.clone(), bytes.len(), false);
    let storage = storage
        .with_chunk_size(NonZeroUsize::new(4).unwrap())
        .with_http_concurrency_limit(HttpConcurrencyLimitLayer::new(1).unwrap());
    let mut reader = storage.open_sequential("object".into()).await.unwrap();
    let mut output = reader.next().await.unwrap().unwrap().to_vec();
    assert!(!reader.capability().read_with_range);
    assert_eq!(
        reader.read_range(12..17).await.unwrap_err().kind(),
        open_arrow_access_core::ErrorKind::Unsupported
    );
    assert_eq!(
        reader
            .read_ranges(vec![12..17, 0..1])
            .await
            .unwrap_err()
            .kind(),
        open_arrow_access_core::ErrorKind::Unsupported
    );
    while let Some(chunk) = reader.next().await.unwrap() {
        output.extend_from_slice(&chunk);
    }
    assert_eq!(output, bytes);
    assert_eq!(*requests.0.lock().unwrap(), [(Method::GET, None)]);
}

#[tokio::test]
async fn ordinary_and_known_length_opens_retain_independent_ranges() {
    for hint in [false, true] {
        let bytes: Vec<_> = (0..32).collect();
        let (mut storage, requests) = storage(bytes.clone(), bytes.len(), false);
        let mut reader = if hint {
            storage = storage.with_content_lengths([("object".into(), bytes.len() as u64)]);
            storage.open_sequential("object".into()).await.unwrap()
        } else {
            storage.open("object".into()).await.unwrap()
        };
        assert!(reader.capability().read_with_range);
        assert_eq!(
            reader.read_range(12..17).await.unwrap().as_ref(),
            &bytes[12..17]
        );
        let mut output = Vec::new();
        while let Some(chunk) = reader.next().await.unwrap() {
            output.extend_from_slice(&chunk);
        }
        assert_eq!(output, bytes);
        assert_eq!(
            reader.read_range(12..17).await.unwrap().as_ref(),
            &bytes[12..17]
        );
        assert!(reader.next().await.unwrap().is_none());
        let requests = requests.0.lock().unwrap();
        let reads = if hint {
            &requests[..]
        } else {
            assert_eq!(requests[0], (Method::HEAD, None));
            &requests[1..]
        };
        assert_eq!(
            reads,
            [
                (Method::GET, Some("bytes=12-16".into())),
                (Method::GET, Some("bytes=0-31".into())),
                (Method::GET, Some("bytes=12-16".into())),
            ]
        );
    }
}

#[tokio::test]
async fn ordinary_empty_reader_finishes_without_starting_a_get() {
    let (storage, requests) = storage(Vec::new(), 0, false);
    let mut reader = storage.open("object".into()).await.unwrap();
    assert!(reader.capability().read_with_range);
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.read_range(0..0).await.unwrap().is_empty());
    assert_eq!(*requests.0.lock().unwrap(), [(Method::HEAD, None)]);
}

#[tokio::test]
async fn range_validation_errors_leave_the_sequential_cursor_usable() {
    let bytes: Vec<_> = (0..32).collect();
    let (storage, _) = storage(bytes.clone(), bytes.len(), false);
    let storage = storage.with_chunk_size(NonZeroUsize::new(4).unwrap());
    let mut reader = storage.open("object".into()).await.unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), &bytes[..4]);
    let position = reader.position();
    assert_eq!(
        reader.read_range(31..33).await.unwrap_err().kind(),
        open_arrow_access_core::ErrorKind::InvalidInput
    );
    assert_eq!(
        reader
            .read_ranges(vec![0..1, 31..33])
            .await
            .unwrap_err()
            .kind(),
        open_arrow_access_core::ErrorKind::InvalidInput
    );
    assert_eq!(reader.position(), position);
    assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), &bytes[4..8]);
}

#[tokio::test]
async fn explicit_chunk_options_keep_known_length_partitioned_io() {
    let bytes: Vec<_> = (0..32).collect();
    let (storage, requests) = storage(bytes.clone(), bytes.len(), false);
    let reader = storage
        .open_sequential(
            ByteReadRequest::new("object").with_options(ByteReadOptions {
                io_chunk_size: NonZeroUsize::new(4),
                io_concurrency: NonZeroUsize::new(2).unwrap(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(read_all(reader, "test").await.unwrap(), bytes);
    let requests = requests.0.lock().unwrap();
    assert_eq!(requests[0], (Method::HEAD, None));
    assert_eq!(requests.len(), 9);
    assert!(
        requests[1..]
            .iter()
            .all(|(method, range)| *method == Method::GET && range.is_some())
    );
}

#[tokio::test]
async fn truncated_sequential_response_errors_once_and_then_terminates() {
    let (storage, _) = storage(vec![1, 2], 4, false);
    let mut reader = storage.open_sequential("object".into()).await.unwrap();
    assert!(reader.next().await.is_err());
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn oversized_sequential_response_errors_once_and_then_terminates() {
    let (storage, _) = storage(vec![1, 2, 3, 4], 2, false);
    let mut reader = storage.open_sequential("object".into()).await.unwrap();
    assert!(reader.next().await.is_err());
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn dropping_a_sequential_reader_releases_its_http_permit() {
    let (storage, requests) = storage(vec![1, 2, 3, 4], 4, true);
    let storage = storage.with_http_concurrency_limit(HttpConcurrencyLimitLayer::new(1).unwrap());
    let mut first = storage.open_sequential("object".into()).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), first.next())
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            storage.open_sequential("object".into())
        )
        .await
        .is_err()
    );
    assert_eq!(requests.0.lock().unwrap().len(), 1);
    drop(first);
    let second = tokio::time::timeout(
        Duration::from_secs(1),
        storage.open_sequential("object".into()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(requests.0.lock().unwrap().len(), 2);
    drop(second);
}

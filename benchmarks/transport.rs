//! In-process HTTP replay: adapter/fragmentation costs, not network throughput.
use futures::stream;
use http::{Method, Request, Response, StatusCode};
use open_arrow_access_core::Bytes;
use opendal::{Buffer, HttpBody, HttpTransport};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Clone)]
pub struct Transport {
    data: Bytes,
    chunk: usize,
    latency_ms: u64,
    requests: Arc<AtomicUsize>,
    bytes: Arc<AtomicUsize>,
}
impl Transport {
    pub fn new(data: Vec<u8>, chunk: usize, latency_ms: usize) -> Self {
        assert!(chunk > 0);
        Self {
            data: data.into(),
            chunk,
            latency_ms: latency_ms as u64,
            requests: Arc::default(),
            bytes: Arc::default(),
        }
    }
    pub fn reset(&self) {
        self.requests.store(0, Ordering::Relaxed);
        self.bytes.store(0, Ordering::Relaxed);
    }
    pub fn requests(&self) -> usize {
        self.requests.load(Ordering::Relaxed)
    }
    pub fn bytes(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }
}
impl HttpTransport for Transport {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        self.requests.fetch_add(1, Ordering::Relaxed);
        if self.latency_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(self.latency_ms)).await;
        }
        let mut response = Response::builder()
            .status(StatusCode::OK)
            .header("etag", "\"benchmark\"");
        if request.method() == Method::HEAD {
            return Ok(response
                .header("content-length", self.data.len())
                .body(HttpBody::new(stream::empty(), Some(0)))
                .unwrap());
        }
        assert_eq!(request.method(), Method::GET);
        let bytes = if let Some(range) = request.headers().get("range") {
            let (start, end) = range
                .to_str()
                .unwrap()
                .strip_prefix("bytes=")
                .unwrap()
                .split_once('-')
                .unwrap();
            let start: usize = start.parse().unwrap();
            let end = if end.is_empty() {
                self.data.len()
            } else {
                (end.parse::<usize>().unwrap() + 1).min(self.data.len())
            };
            assert!(start < end);
            response = response.status(StatusCode::PARTIAL_CONTENT).header(
                "content-range",
                format!("bytes {}-{}/{}", start, end - 1, self.data.len()),
            );
            self.data.slice(start..end)
        } else {
            self.data.clone()
        };
        let len = bytes.len();
        self.bytes.fetch_add(len, Ordering::Relaxed);
        Ok(response
            .header("content-length", len)
            .body(HttpBody::new(
                stream::iter(Buffer::from(bytes).chunks(self.chunk).map(Ok)),
                Some(len as u64),
            ))
            .unwrap())
    }
}

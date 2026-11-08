//! Deterministic HTTP fixtures shared by provider integration tests.

use std::{
    collections::HashMap,
    sync::{
        Arc, LazyLock, Mutex, Once, Weak,
        atomic::{AtomicUsize, Ordering},
    },
};

use futures::stream;
use http::{HeaderMap, Method, Request, Response, StatusCode};
use open_arrow_access_core::Bytes;
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter};

static FIXTURES: LazyLock<Mutex<HashMap<String, Weak<State>>>> = LazyLock::new(Mutex::default);

#[derive(Clone, Debug)]
pub struct CapturedRequest {
    pub method: Method,
    pub path: String,
    pub headers: HeaderMap,
}

#[derive(Default)]
struct State {
    routes: Mutex<HashMap<(Method, String), Route>>,
    requests: Mutex<Vec<CapturedRequest>>,
}

#[derive(Clone)]
enum Route {
    Response(StatusCode, Bytes, bool),
    File(Bytes),
}

/// Each fixture gets its own host; parallel cases cannot consume each other's routes.
/// Keep this handle alive until all associated readers have finished.
pub struct HttpFixture {
    endpoint: String,
    state: Arc<State>,
}

impl Default for HttpFixture {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpFixture {
    pub fn new() -> Self {
        static INSTALL: Once = Once::new();
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        INSTALL.call_once(|| HttpTransporter::install_default(FixtureTransport));
        let host = format!(
            "http-fixture-{}.invalid",
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let state = Arc::new(State::default());
        FIXTURES
            .lock()
            .unwrap()
            .insert(host.clone(), Arc::downgrade(&state));
        Self {
            endpoint: format!("https://{host}"),
            state,
        }
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn respond(&self, path: &str, status: StatusCode, bytes: impl Into<Bytes>) {
        self.respond_for(Method::GET, path, status, bytes);
    }

    pub fn respond_for(
        &self,
        method: Method,
        path: &str,
        status: StatusCode,
        bytes: impl Into<Bytes>,
    ) {
        self.state.routes.lock().unwrap().insert(
            (method, path.into()),
            Route::Response(status, bytes.into(), true),
        );
    }

    pub fn respond_chunked(&self, path: &str, status: StatusCode, bytes: impl Into<Bytes>) {
        self.state.routes.lock().unwrap().insert(
            (Method::GET, path.into()),
            Route::Response(status, bytes.into(), false),
        );
    }

    pub fn file(&self, path: &str, bytes: impl Into<Bytes>) {
        let bytes = bytes.into();
        let mut routes = self.state.routes.lock().unwrap();
        routes.insert((Method::HEAD, path.into()), Route::File(bytes.clone()));
        routes.insert((Method::GET, path.into()), Route::File(bytes));
    }

    pub fn requests(&self) -> Vec<CapturedRequest> {
        self.state.requests.lock().unwrap().clone()
    }
}

struct FixtureTransport;
impl HttpTransport for FixtureTransport {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        let state = FIXTURES
            .lock()
            .unwrap()
            .get(request.uri().host().unwrap())
            .and_then(Weak::upgrade)
            .expect("unexpected request or expired HTTP fixture");
        let path = request.uri().path_and_query().unwrap().as_str();
        state.requests.lock().unwrap().push(CapturedRequest {
            method: request.method().clone(),
            path: path.into(),
            headers: request.headers().clone(),
        });
        let route = state
            .routes
            .lock()
            .unwrap()
            .get(&(request.method().clone(), path.into()))
            .cloned()
            .unwrap_or_else(|| panic!("unexpected {} request: {path}", request.method()));
        match route {
            Route::Response(status, bytes, length) => Ok(response(status, bytes, length)),
            Route::File(bytes) => {
                if request.method() == Method::HEAD {
                    let mut output = response(StatusCode::OK, Bytes::new(), true);
                    output
                        .headers_mut()
                        .insert("content-length", bytes.len().into());
                    return Ok(output);
                }
                assert_eq!(request.method(), Method::GET);
                let Some(range) = request.headers().get("range") else {
                    return Ok(response(StatusCode::OK, bytes, true));
                };
                let (start, end) = range
                    .to_str()
                    .unwrap()
                    .strip_prefix("bytes=")
                    .unwrap()
                    .split_once('-')
                    .unwrap();
                let start: usize = start.parse().unwrap();
                let end = if end.is_empty() {
                    bytes.len()
                } else {
                    (end.parse::<usize>().unwrap() + 1).min(bytes.len())
                };
                if start >= end {
                    return Ok(response(
                        StatusCode::RANGE_NOT_SATISFIABLE,
                        Bytes::new(),
                        true,
                    ));
                }
                let mut output =
                    response(StatusCode::PARTIAL_CONTENT, bytes.slice(start..end), true);
                output.headers_mut().insert(
                    "content-range",
                    format!("bytes {start}-{}/{}", end - 1, bytes.len())
                        .parse()
                        .unwrap(),
                );
                Ok(output)
            }
        }
    }
}

fn response(status: StatusCode, bytes: Bytes, length: bool) -> Response<HttpBody> {
    let size = length.then_some(bytes.len() as u64);
    let mut output = Response::builder().status(status);
    if let Some(size) = size {
        output = output.header("content-length", size);
    }
    output
        .body(HttpBody::new(
            stream::iter(Buffer::from(bytes).chunks(3).map(Ok)),
            size,
        ))
        .unwrap()
}

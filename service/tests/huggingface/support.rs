use std::{
    collections::{BTreeMap, HashMap},
    ops::Range,
    sync::{
        Arc, LazyLock, Mutex, Once,
        atomic::{AtomicUsize, Ordering::Relaxed},
    },
};

use futures::stream;
use http::{Request, Response, StatusCode};
use open_arrow_access_core::Bytes;
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter};
use serde_json::json;

use super::super::{
    HuggingFaceAccessor, HuggingFaceOptions, HuggingFaceRepository, HuggingFaceService,
};

// This integration-test binary has one default transport. Each fixture gets
// its own host/handler, so parallel tests never share request state or use the
// public network. Install it before constructing any service in this binary.
static TRANSPORTS: LazyLock<Mutex<HashMap<String, HttpTransporter>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

struct FixtureTransport;

impl HttpTransport for FixtureTransport {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        let handler = TRANSPORTS
            .lock()
            .unwrap()
            .get(request.uri().host().unwrap())
            .expect("unexpected network request")
            .clone();
        handler.fetch(request).await
    }
}

pub(super) fn install_mock_transport() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| HttpTransporter::install_default(FixtureTransport));
}

pub(super) async fn service_with(transport: impl HttpTransport) -> HuggingFaceService {
    install_mock_transport();
    static NEXT_HOST: AtomicUsize = AtomicUsize::new(0);
    let host = format!("fixture-{}.invalid", NEXT_HOST.fetch_add(1, Relaxed));
    TRANSPORTS
        .lock()
        .unwrap()
        .insert(host.clone(), HttpTransporter::new(transport));
    HuggingFaceService::new(
        HuggingFaceOptions::default()
            .with_endpoint(format!("https://{host}"))
            .with_token(TOKEN),
    )
    .await
    .unwrap()
}

pub(super) const TOKEN: &str = "fixture-only-token";

#[derive(Clone, Debug)]
pub(super) struct Captured {
    pub(super) range: Option<String>,
}

#[derive(Clone, Default)]
pub(super) struct MockHub {
    files: Arc<BTreeMap<String, Bytes>>,
    requests: Arc<Mutex<Vec<Captured>>>,
}

impl MockHub {
    pub(super) fn with_files(files: impl IntoIterator<Item = (String, Bytes)>) -> Self {
        Self {
            files: Arc::new(files.into_iter().collect()),
            ..Self::default()
        }
    }

    pub(super) fn file(path: &str, bytes: impl Into<Bytes>) -> Self {
        Self::with_files([(format!("data/{path}"), bytes.into())])
    }

    pub(super) fn request_count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }

    pub(super) async fn accessor_with_transport(
        transport: impl HttpTransport,
    ) -> HuggingFaceAccessor {
        service_with(transport)
            .await
            .repository(
                HuggingFaceRepository::new("owner/example")
                    .with_revision("refs/convert/parquet")
                    .with_root("data"),
            )
            .await
            .unwrap()
    }

    pub(super) fn captured(&self) -> Vec<Captured> {
        self.requests.lock().unwrap().clone()
    }
}

pub(super) fn response(status: StatusCode, bytes: impl Into<Bytes>) -> Response<HttpBody> {
    let bytes = bytes.into();
    let len = bytes.len() as u64;
    // Exercise fragmented HTTP bodies, including output chunks that span buffers.
    Response::builder()
        .status(status)
        .header("content-length", len)
        .body(HttpBody::new(
            stream::iter(Buffer::from(bytes).chunks(3).map(Ok)),
            Some(len),
        ))
        .unwrap()
}

impl HttpTransport for MockHub {
    async fn fetch(&self, req: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        tokio::task::yield_now().await;
        let uri = req.uri().to_string();
        let range = req
            .headers()
            .get("range")
            .map(|v| v.to_str().unwrap().to_owned());
        let form = String::from_utf8(req.body().to_bytes().to_vec()).unwrap();
        assert_eq!(req.headers()["authorization"], format!("Bearer {TOKEN}"));
        self.requests.lock().unwrap().push(Captured {
            range: range.clone(),
        });
        if uri.contains("/paths-info/") {
            let path = form
                .strip_prefix("paths=")
                .unwrap()
                .split('&')
                .next()
                .unwrap();
            let value = if let Some(bytes) = self.files.get(path) {
                json!([{"type":"file","path":path,"size":bytes.len(),"oid":"fixture"}])
            } else if self
                .files
                .keys()
                .any(|key| key.starts_with(&format!("{}/", path.trim_end_matches('/'))))
            {
                json!([{"type":"directory","path":path.trim_end_matches('/'),"oid":"fixture"}])
            } else {
                json!([])
            };
            return Ok(response(StatusCode::OK, Bytes::from(value.to_string())));
        }
        if uri.contains("/tree/") {
            let recursive = uri.contains("recursive=True");
            let values: Vec<_> = self
                .files
                .iter()
                .filter(|(path, _)| {
                    recursive || !path.strip_prefix("data/").unwrap().contains('/')
                })
                .map(|(path, bytes)| {
                    json!({"type":"file","path":path,"size":bytes.len(),"oid":"fixture"})
                })
                .collect();
            let page2 = uri.contains("cursor=second");
            let mut page = if page2 {
                values[values.len().min(1)..].to_vec()
            } else {
                values[..values.len().min(1)].to_vec()
            };
            if !page2 {
                page.push(json!({"type":"directory", "path":"data/nested"}));
            }
            let mut output = response(
                StatusCode::OK,
                Bytes::from(serde_json::to_string(&page).unwrap()),
            );
            if !page2 && values.len() > 1 {
                let recursive_query = if recursive { "&recursive=True" } else { "" };
                output.headers_mut().insert(
                    "link",
                    format!("<https://hub.invalid/api/datasets/owner/example/tree/refs%2Fconvert%2Fparquet/data?cursor=second{recursive_query}>; rel=\"next\"")
                        .parse()
                        .unwrap(),
                );
            }
            return Ok(output);
        }
        if let Some((_, path)) = uri
            .split_once("/resolve/")
            .and_then(|(_, rest)| rest.split_once('/'))
        {
            let bytes = self.files.get(path).unwrap();
            let selected = range
                .as_deref()
                .map_or(0..bytes.len(), |value| request_range(value, bytes.len()));
            let (start, end) = (selected.start, selected.end);
            let mut output = response(
                if range.is_some() {
                    StatusCode::PARTIAL_CONTENT
                } else {
                    StatusCode::OK
                },
                bytes.slice(start..end),
            );
            if range.is_some() {
                output.headers_mut().insert(
                    "content-range",
                    format!("bytes {start}-{}/{len}", end - 1, len = bytes.len())
                        .parse()
                        .unwrap(),
                );
            }
            return Ok(output);
        }
        panic!("unexpected request: {uri}")
    }
}

pub(super) fn request_range(header: &str, len: usize) -> Range<usize> {
    let (start, end) = header
        .strip_prefix("bytes=")
        .unwrap()
        .split_once('-')
        .unwrap();
    let end = if end.is_empty() {
        len
    } else {
        end.parse::<usize>().unwrap() + 1
    };
    start.parse().unwrap()..end
}

use std::{
    collections::{BTreeMap, HashMap},
    ops::Range,
    sync::{
        Arc, LazyLock, Mutex, Once,
        atomic::{AtomicUsize, Ordering::Relaxed},
    },
};

use arrow_array::types::Int64Type;
use futures::stream;
use http::{Request, Response, StatusCode};
use open_arrow_access_core::Bytes;
use open_arrow_access_test_support::{fixture, record::collect_column};
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter};
use serde::Deserialize;
use serde_json::Value;

use super::super::{HuggingFaceDatasetRequest, HuggingFaceOptions, HuggingFaceService};

pub(super) const COMMIT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const TOKEN: &str = "fixture-only-token";

// Route each fixture to its own host so parallel tests never share request state.
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

#[derive(Deserialize)]
struct ExportEntry {
    fixture: String,
    metadata: Value,
}

struct Captured {
    uri: String,
    range: Option<String>,
}

#[derive(Clone)]
pub(super) struct ExportHub {
    files: Arc<BTreeMap<String, Bytes>>,
    pub(super) metadata: Bytes,
    catalog: Arc<Vec<Value>>,
    pub(super) full_status: StatusCode,
    requests: Arc<Mutex<Vec<Captured>>>,
}

impl ExportHub {
    pub(super) async fn new(name: &str) -> Self {
        let entries: Vec<ExportEntry> =
            serde_json::from_slice(&fixture!("huggingface/exports", name).await).unwrap();
        let mut files = BTreeMap::new();
        let mut catalog = Vec::new();
        for entry in entries {
            let bytes = fixture!("parquet", &entry.fixture).await;
            assert_eq!(entry.metadata["size"].as_u64().unwrap(), bytes.len() as u64);
            files.insert(
                entry.metadata["path"].as_str().unwrap().to_owned(),
                Bytes::from(bytes),
            );
            catalog.push(entry.metadata);
        }
        Self {
            files: Arc::new(files),
            metadata: Bytes::from(fixture!("huggingface", "revision.json").await),
            catalog: Arc::new(catalog),
            full_status: StatusCode::OK,
            requests: Arc::default(),
        }
    }

    pub(super) async fn service(&self) -> HuggingFaceService {
        static INSTALL: Once = Once::new();
        INSTALL.call_once(|| HttpTransporter::install_default(FixtureTransport));
        static NEXT_HOST: AtomicUsize = AtomicUsize::new(0);
        let host = format!("fixture-{}.invalid", NEXT_HOST.fetch_add(1, Relaxed));
        TRANSPORTS
            .lock()
            .unwrap()
            .insert(host.clone(), HttpTransporter::new(self.clone()));
        HuggingFaceService::new(
            HuggingFaceOptions::default()
                .with_endpoint(format!("https://{host}"))
                .with_token(TOKEN),
        )
        .await
        .unwrap()
    }

    pub(super) async fn read_ids(&self, request: HuggingFaceDatasetRequest) -> Vec<i64> {
        let reader = self.service().await.dataset(request).await.unwrap();
        collect_column::<Int64Type>(reader, 0).await
    }

    pub(super) fn requests(&self) -> Vec<String> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|r| r.uri.clone())
            .collect()
    }

    pub(super) fn requested(&self, fragment: &str) -> bool {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.uri.contains(fragment))
    }

    pub(super) fn clear_requests(&self) {
        self.requests.lock().unwrap().clear();
    }

    pub(super) fn ranges(&self) -> Vec<Range<usize>> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter_map(|r| {
                r.range
                    .as_deref()
                    .map(|value| request_range(value, usize::MAX))
            })
            .collect()
    }
}

impl HttpTransport for ExportHub {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        tokio::task::yield_now().await;
        assert_eq!(
            request.headers()["authorization"],
            format!("Bearer {TOKEN}")
        );
        assert!(
            request.headers()["user-agent"]
                .to_str()
                .unwrap()
                .starts_with("open-arrow-access/")
        );
        let uri = request.uri().to_string();
        let range = request
            .headers()
            .get("range")
            .map(|v| v.to_str().unwrap().to_owned());
        self.requests.lock().unwrap().push(Captured {
            uri: uri.clone(),
            range: range.clone(),
        });
        if uri.contains("/revision/") {
            assert!(uri.contains("refs%2Fconvert%2Fparquet"));
            assert!(uri.ends_with("expand=sha"));
            return Ok(response(StatusCode::OK, self.metadata.clone()));
        }
        assert!(
            uri.contains(COMMIT),
            "membership and file I/O must use the pinned commit"
        );
        if let Some((_, rest)) = uri.split_once("/tree/") {
            assert!(!uri.contains("recursive=True"));
            let prefix = rest.split_once('/').unwrap().1.split('?').next().unwrap();
            if prefix == "default/train" && self.full_status != StatusCode::OK {
                return Ok(response(self.full_status, Bytes::new()));
            }
            // Reverse path order and paginate to exercise provider ordering.
            let prefix = format!("{prefix}/");
            let mut files: Vec<_> = self
                .catalog
                .iter()
                .filter(|entry| {
                    let path = entry["path"].as_str().unwrap();
                    path.starts_with(&prefix) && self.files.contains_key(path)
                })
                .collect();
            files.sort_by(|a, b| b["path"].as_str().cmp(&a["path"].as_str()));
            if files.is_empty() {
                return Ok(response(StatusCode::NOT_FOUND, Bytes::new()));
            }
            let page = if uri.contains("cursor=second") {
                &files[1..]
            } else {
                &files[..1]
            };
            let mut output = response(
                StatusCode::OK,
                Bytes::from(serde_json::to_vec(page).unwrap()),
            );
            if !uri.contains("cursor=second") && files.len() > 1 {
                output.headers_mut().insert(
                    "link",
                    format!("<{uri}&cursor=second>; rel=\"next\"")
                        .parse()
                        .unwrap(),
                );
            }
            return Ok(output);
        }
        if uri.contains("/paths-info/") {
            let form = String::from_utf8(request.body().to_bytes().to_vec()).unwrap();
            let path = form
                .strip_prefix("paths=")
                .unwrap()
                .split('&')
                .next()
                .unwrap();
            let files: Vec<_> = self
                .catalog
                .iter()
                .filter(|entry| entry["path"] == path && self.files.contains_key(path))
                .collect();
            return Ok(response(
                StatusCode::OK,
                Bytes::from(serde_json::to_vec(&files).unwrap()),
            ));
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
            let status = if range.is_some() {
                StatusCode::PARTIAL_CONTENT
            } else {
                StatusCode::OK
            };
            let mut output = response(status, bytes.slice(start..end));
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

fn response(status: StatusCode, bytes: Bytes) -> Response<HttpBody> {
    let len = bytes.len() as u64;
    Response::builder()
        .status(status)
        .header("content-length", len)
        .body(HttpBody::new(
            stream::iter(Buffer::from(bytes).chunks(3).map(Ok)),
            Some(len),
        ))
        .unwrap()
}

fn request_range(header: &str, len: usize) -> Range<usize> {
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

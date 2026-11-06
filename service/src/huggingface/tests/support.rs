use std::{
    collections::BTreeMap,
    ops::Range,
    sync::{Arc, Mutex},
};

use futures::stream;
use http::{Request, Response, StatusCode};
use open_arrow_access_core::Bytes;
use open_arrow_access_storage::OpenDalAccessor;
use opendal::{
    Buffer, HttpBody, HttpTransport, HttpTransporter, OperationContext, Operator, services::Hf,
};
use serde_json::json;

use super::super::HuggingFaceAccessor;

pub(super) const TOKEN: &str = "fixture-only-token";

#[derive(Clone, Debug)]
pub(super) struct Captured {
    pub(super) uri: String,
    pub(super) range: Option<String>,
    pub(super) form: String,
}

#[derive(Clone)]
pub(super) enum HubFailure {
    Status(StatusCode),
    Transport(opendal::ErrorKind),
}

#[derive(Clone, Default)]
pub(super) struct MockHub {
    files: Arc<BTreeMap<String, Bytes>>,
    requests: Arc<Mutex<Vec<Captured>>>,
    failure: Option<(&'static str, HubFailure)>,
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

    pub(super) fn with_failure(mut self, route: &'static str, failure: HubFailure) -> Self {
        self.failure = Some((route, failure));
        self
    }

    pub(super) fn accessor(&self) -> HuggingFaceAccessor {
        Self::accessor_with_transport(self.clone())
    }

    pub(super) fn accessor_with_transport(transport: impl HttpTransport) -> HuggingFaceAccessor {
        let builder = Hf::default()
            .repo_type("dataset")
            .repo_id("owner/example")
            .revision("refs/convert/parquet")
            .root("data")
            .endpoint("https://hub.invalid")
            .token(TOKEN)
            .download_mode("http");
        let context = OperationContext::new().with_http_transport(HttpTransporter::new(transport));
        let operator = Operator::new(builder).unwrap().with_context(context);
        HuggingFaceAccessor {
            inner: OpenDalAccessor::new(operator),
        }
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
            uri: uri.clone(),
            range: range.clone(),
            form: form.clone(),
        });
        if let Some((route, failure)) = &self.failure
            && uri.contains(route)
        {
            return match failure {
                HubFailure::Status(status) => Ok(response(*status, Bytes::new())),
                HubFailure::Transport(kind) => {
                    Err(opendal::Error::new(*kind, "fixture transport failed"))
                }
            };
        }
        if uri.contains("/paths-info/") {
            let path = form
                .strip_prefix("paths=")
                .unwrap()
                .split('&')
                .next()
                .unwrap();
            if path.ends_with("denied.bin") {
                return Ok(response(StatusCode::FORBIDDEN, Bytes::new()));
            }
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
            let values: Vec<_> = self.files.iter().map(|(path, bytes)| json!({"type":"file","path":path,"size":bytes.len(),"oid":"fixture"})).collect();
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
                output.headers_mut().insert("link", "<https://hub.invalid/api/datasets/owner/example/tree/refs%2Fconvert%2Fparquet/data?cursor=second>; rel=\"next\"".parse().unwrap());
            }
            return Ok(output);
        }
        if let Some((_, path)) = uri.split_once("/resolve/refs%2Fconvert%2Fparquet/") {
            let bytes = self.files.get(path).unwrap();
            let selected = range
                .as_deref()
                .map_or(0..bytes.len(), |value| request_range(value, bytes.len()));
            let (start, end) = (selected.start, selected.end);
            if path.ends_with("fail.bin") && start >= 4 {
                return Err(opendal::Error::new(
                    opendal::ErrorKind::Unexpected,
                    "fixture transport failed",
                ));
            }
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

pub(super) fn assert_ranges(requests: &[Captured], expected: &[&str]) {
    let mut actual: Vec<_> = requests.iter().filter_map(|r| r.range.as_deref()).collect();
    let mut expected = expected.to_vec();
    actual.sort_unstable();
    expected.sort_unstable();
    assert_eq!(actual, expected);
}

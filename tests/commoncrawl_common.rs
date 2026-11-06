use futures::{TryStreamExt, stream};
use http::{Request, Response, StatusCode};
use open_arrow_access::{
    Accessor, ByteAccessor, Bytes, ErrorKind,
    service::{CommonCrawl, create_http_accessor},
    storage::{OpenDalAccessor, opendal},
};
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter, OperationContext, Operator};
use std::sync::{Arc, Mutex};

const MANIFEST: &str = "crawl-data/CC-MAIN-2022-40/warc.paths.gz";

#[test]
fn crawl_identity_and_mirror_urls_are_canonical() {
    let source =
        CommonCrawl::from_url("https://mirror.example/crawl-data/CC-MAIN-2022-40/").unwrap();
    assert_eq!(source.crawl(), "CC-MAIN-2022-40");
    assert_eq!(source.origin(), "https://mirror.example");
    assert_eq!(
        CommonCrawl::new("CC-MAIN-2022-40").unwrap().origin(),
        "https://data.commoncrawl.org"
    );
    for crawl in ["../CC-MAIN-2022-40", "CC-MAIN-22-40", "CC-MAIN-2022-400"] {
        assert_eq!(
            CommonCrawl::new(crawl).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
    for value in [
        "https://host/crawl-data/CC-MAIN-2022-40/?x=1",
        "https://user:pass@host/crawl-data/CC-MAIN-2022-40/",
        "https://host/x/../crawl-data/CC-MAIN-2022-40/",
        "https://host/crawl-data/%43C-MAIN-2022-40/",
        "https://host/crawl-data//CC-MAIN-2022-40/",
        "https://host/crawl-data/CC-MAIN-2022-40//",
        "file:///crawl-data/CC-MAIN-2022-40/",
    ] {
        assert_eq!(
            CommonCrawl::from_url(value).unwrap_err().kind(),
            ErrorKind::InvalidInput,
            "{value}"
        );
    }
}

#[test]
fn http_configuration_is_lazy_and_listing_is_unsupported() {
    let bytes = create_http_accessor("https://data.commoncrawl.org/").unwrap();
    assert!(bytes.capability().read);
    assert!(bytes.capability().read_with_range);
    assert!(!bytes.capability().list);
    for origin in [
        "file:///tmp/",
        "https://user:pass@host/",
        "https://host/path",
        "https://host/a/../",
        "https://host/%2f",
        "https://host/?x=1",
    ] {
        assert_eq!(
            create_http_accessor(origin).unwrap_err().kind(),
            ErrorKind::InvalidInput,
            "{origin}"
        );
        assert_eq!(
            CommonCrawl::new("CC-MAIN-2022-40")
                .unwrap()
                .with_origin(origin)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
    }
}

#[derive(Clone)]
struct MockHttp {
    manifest: Bytes,
    requests: Arc<Mutex<Vec<(String, String)>>>,
}

impl HttpTransport for MockHttp {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        let uri = request.uri().to_string();
        self.requests
            .lock()
            .unwrap()
            .push((request.method().to_string(), uri.clone()));
        assert_eq!(uri, format!("https://data.commoncrawl.org/{MANIFEST}"));
        let len = self.manifest.len();
        let mut response = Response::builder().header("content-length", len);
        let body = if request.method() == http::Method::HEAD {
            Bytes::new()
        } else {
            let value = request.headers()["range"].to_str().unwrap();
            let (start, end) = value
                .strip_prefix("bytes=")
                .unwrap()
                .split_once('-')
                .unwrap();
            let start = start.parse::<usize>().unwrap();
            let end = if end.is_empty() {
                len
            } else {
                end.parse::<usize>().unwrap() + 1
            };
            response = response
                .status(StatusCode::PARTIAL_CONTENT)
                .header("content-range", format!("bytes {start}-{}/{len}", end - 1));
            self.manifest.slice(start..end)
        };
        let body_len = body.len() as u64;
        response
            .body(HttpBody::new(
                stream::iter([Ok(Buffer::from(body))]),
                Some(body_len),
            ))
            .map_err(|e| {
                opendal::Error::new(opendal::ErrorKind::Unexpected, "mock response").set_source(e)
            })
    }
}

#[tokio::test]
async fn origin_rooted_http_uses_injected_transport_and_rejects_aliases() {
    let manifest = Bytes::from_static(include_bytes!("../testdata/commoncrawl/one.paths"));
    let transport = MockHttp {
        manifest: manifest.clone(),
        requests: Arc::default(),
    };
    let calls = transport.requests.clone();
    let operator =
        Operator::new(opendal::services::Http::default().endpoint("https://data.commoncrawl.org"))
            .unwrap()
            .with_context(
                OperationContext::new().with_http_transport(HttpTransporter::new(transport)),
            );
    let bytes = OpenDalAccessor::new(operator);
    assert_eq!(
        bytes.list("".into()).await.err().unwrap().kind(),
        ErrorKind::Unsupported
    );
    for path in [" /a", "/a", "a//b", "a/../b", "a/"] {
        assert_eq!(
            bytes.open(path.into()).await.err().unwrap().kind(),
            ErrorKind::InvalidInput,
            "{path}"
        );
    }
    let mut reader = bytes.open(MANIFEST.into()).await.unwrap();
    let mut output = Vec::new();
    while let Some(chunk) = reader.next().await.unwrap() {
        output.extend_from_slice(&chunk);
    }
    assert_eq!(output, manifest);
    assert_eq!(
        calls
            .lock()
            .unwrap()
            .iter()
            .map(|(method, _)| method.as_str())
            .collect::<Vec<_>>(),
        ["HEAD", "GET"]
    );
}

#[tokio::test]
async fn generic_listing_accepts_virtual_directory_prefixes() {
    let operator = Operator::new(opendal::services::Memory::default()).unwrap();
    operator
        .write("train/a.warc", Bytes::from_static(b"data"))
        .await
        .unwrap();
    let bytes = OpenDalAccessor::new(operator);
    for prefix in ["train", "train/"] {
        let files = bytes
            .list(prefix.into())
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(files, vec!["train/a.warc".into()]);
    }
}

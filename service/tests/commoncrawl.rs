use http::{Request, Response};
use open_arrow_access_core::{Accessor, ByteAccessor, Bytes, ErrorKind};
use open_arrow_access_service::commoncrawl::CommonCrawl;
use open_arrow_access_storage::{OpenDalAccessor, opendal};
use open_arrow_access_test_support::http::file_response;

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
        Ok(file_response(&request, self.manifest.clone()))
    }
}

#[tokio::test]
async fn origin_rooted_http_uses_injected_transport_and_rejects_aliases() {
    let manifest = Bytes::from_static(include_bytes!("../../testdata/commoncrawl/one.paths"));
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

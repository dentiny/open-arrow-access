use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::{
        Once,
        atomic::{AtomicUsize, Ordering},
    },
};

use futures::{TryStreamExt, stream};
use http::{Request, Response, StatusCode};
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadOptions, ByteReadRequest, Bytes, ErrorKind, ListRequest,
};
use open_arrow_access_service::zenodo::ZenodoService;
use open_arrow_access_test_support::{collect_bytes, fixture};
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter};

const ID: NonZeroU64 = NonZeroU64::new(3551612).unwrap();
static FILE_READS: AtomicUsize = AtomicUsize::new(0);
static METADATA_READS: AtomicUsize = AtomicUsize::new(0);
struct Transport;
impl HttpTransport for Transport {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        let host = request.uri().host().unwrap();
        if request.uri().path() == "/api/records/3551612" {
            METADATA_READS.fetch_add(1, Ordering::SeqCst);
            let status = match host {
                "forbidden.invalid" => StatusCode::FORBIDDEN,
                "missing.invalid" => StatusCode::NOT_FOUND,
                "limited.invalid" => StatusCode::TOO_MANY_REQUESTS,
                _ => StatusCode::OK,
            };
            let name = host.strip_suffix(".invalid").unwrap();
            let name = match name {
                "forbidden" | "missing" | "limited" => "empty",
                other => other,
            };
            return Ok(response(
                status,
                Bytes::from(fixture!("zenodo", &format!("{name}.json")).await),
            ));
        }
        FILE_READS.fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            request.uri().path(),
            "/api/records/3551612/files/paper%20%231%25.pdf/content"
        );
        let data = Bytes::from(fixture!("zenodo", "attachment.bin").await);
        if request.method() == "HEAD" {
            return Ok(response(StatusCode::OK, data));
        }
        let range = request.headers()["range"]
            .to_str()
            .unwrap()
            .strip_prefix("bytes=")
            .unwrap();
        let (start, end) = range.split_once('-').unwrap();
        let start: usize = start.parse().unwrap();
        let end = if end.is_empty() {
            data.len()
        } else {
            end.parse::<usize>().unwrap() + 1
        };
        let mut result = response(StatusCode::PARTIAL_CONTENT, data.slice(start..end));
        result.headers_mut().insert(
            "content-range",
            format!("bytes {start}-{}/{}", end - 1, data.len())
                .parse()
                .unwrap(),
        );
        Ok(result)
    }
}
fn response(status: StatusCode, data: Bytes) -> Response<HttpBody> {
    let len = data.len() as u64;
    Response::builder()
        .status(status)
        .header("content-length", len)
        .body(HttpBody::new(
            stream::iter(Buffer::from(data).chunks(3).map(Ok)),
            Some(len),
        ))
        .unwrap()
}
fn service(name: &str) -> ZenodoService {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| HttpTransporter::install_default(Transport));
    ZenodoService::with_endpoint(&format!("https://{name}.invalid")).unwrap()
}

#[tokio::test]
async fn record_membership_and_lazy_independent_file_reads() {
    // Other tests only fetch metadata, so these observations isolate attachment I/O.
    let accessor = service("record").record(ID).await.unwrap();
    assert_eq!(FILE_READS.load(Ordering::SeqCst), 0);
    assert_eq!(accessor.id(), ID);
    assert_eq!(accessor.files()[0].size, 8);
    assert_eq!(accessor.files()[0].checksum, "md5:fixture");
    for recursive in [true, false] {
        let listed: Vec<_> = accessor
            .list(ListRequest::new("").with_recursive(recursive))
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(listed[0].locator(), "paper #1%.pdf");
    }
    assert!(accessor.list("nested".into()).await.is_err());
    assert_eq!(
        accessor
            .open("unknown.pdf".into())
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::NotFound
    );
    let options = ByteReadOptions {
        io_chunk_size: Some(NonZeroUsize::new(2).unwrap()),
        ..Default::default()
    };
    let mut reader = accessor
        .open(ByteReadRequest::new("paper #1%.pdf").with_options(options))
        .await
        .unwrap();
    assert_eq!(reader.read_range(1..4).await.unwrap().as_ref(), b"PDF");
    let mut second = accessor.open("paper #1%.pdf".into()).await.unwrap();
    drop(accessor);
    assert_eq!(collect_bytes(reader.as_mut()).await.unwrap(), b"%PDFdata");
    assert_eq!(collect_bytes(second.as_mut()).await.unwrap(), b"%PDFdata");
}

#[tokio::test]
async fn rejects_invalid_metadata_and_bounded_responses() {
    for name in [
        "malformed",
        "wrong-id",
        "missing-files",
        "duplicate",
        "traversal",
    ] {
        assert_eq!(
            service(name).record(ID).await.err().unwrap().kind(),
            ErrorKind::Decode,
            "{name}"
        );
    }
    assert_eq!(
        service("record")
            .with_metadata_limit(8)
            .record(ID)
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Unsupported
    );
    assert!(
        service("empty")
            .record(ID)
            .await
            .unwrap()
            .files()
            .is_empty()
    );
}

#[tokio::test]
async fn maps_http_errors() {
    for (name, kind) in [
        ("forbidden", ErrorKind::PermissionDenied),
        ("missing", ErrorKind::NotFound),
        ("limited", ErrorKind::Io),
    ] {
        assert_eq!(service(name).record(ID).await.err().unwrap().kind(), kind);
    }
}

#[test]
fn validates_endpoint_without_network() {
    for endpoint in [
        "ftp://zenodo.org",
        "https://user:pass@zenodo.org",
        "https://zenodo.org/api",
        "https://zenodo.org?token=x",
        "https://zenodo.org/#x",
    ] {
        assert!(ZenodoService::with_endpoint(endpoint).is_err());
    }
}

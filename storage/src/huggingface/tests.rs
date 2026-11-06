use std::{
    collections::BTreeMap,
    error::Error as _,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use futures::{TryStreamExt, stream};
use http::{Request, Response, StatusCode};
use more_asserts::assert_lt;
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadOptions, ByteReadRequest, Bytes, Error, ErrorKind, ReadOptions,
    ReadRequest,
};
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter, OperationContext, Operator};
use serde_json::json;

use super::{HuggingFaceAccessor, HuggingFaceBuilder};
use crate::backend::OpenDalAccessor;

const TOKEN: &str = "fixture-only-token";

#[derive(Clone, Debug)]
struct Captured {
    uri: String,
    range: Option<String>,
    form: String,
}

#[derive(Clone)]
enum HubFailure {
    Status(StatusCode),
    Transport(opendal::ErrorKind),
}

#[derive(Clone, Default)]
struct MockHub {
    files: Arc<BTreeMap<String, Bytes>>,
    requests: Arc<Mutex<Vec<Captured>>>,
    failure: Option<(&'static str, HubFailure)>,
}

impl MockHub {
    fn with_files(files: impl IntoIterator<Item = (String, Bytes)>) -> Self {
        Self {
            files: Arc::new(files.into_iter().collect()),
            ..Self::default()
        }
    }

    fn with_failure(mut self, route: &'static str, failure: HubFailure) -> Self {
        self.failure = Some((route, failure));
        self
    }

    fn accessor(&self) -> HuggingFaceAccessor {
        let builder = HuggingFaceBuilder::default()
            .repo_type("dataset")
            .repo_id("owner/example")
            .revision("refs/convert/parquet")
            .root("data")
            .endpoint("https://hub.invalid")
            .token(TOKEN)
            .download_mode("http");
        let context =
            OperationContext::new().with_http_transport(HttpTransporter::new(self.clone()));
        let operator = Operator::new(builder).unwrap().with_context(context);
        HuggingFaceAccessor {
            inner: OpenDalAccessor::new(operator),
        }
    }

    fn captured(&self) -> Vec<Captured> {
        self.requests.lock().unwrap().clone()
    }
}

fn response(status: StatusCode, bytes: impl Into<Bytes>) -> Response<HttpBody> {
    let bytes = bytes.into();
    let len = bytes.len() as u64;
    Response::builder()
        .status(status)
        .header("content-length", len)
        .body(HttpBody::new(
            stream::iter([Ok(Buffer::from(bytes))]),
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
            let (start, end) = match range.as_deref() {
                Some(value) => {
                    let (start, end) = value
                        .strip_prefix("bytes=")
                        .unwrap()
                        .split_once('-')
                        .unwrap();
                    (
                        start.parse::<usize>().unwrap(),
                        if end.is_empty() {
                            bytes.len()
                        } else {
                            end.parse::<usize>().unwrap() + 1
                        },
                    )
                }
                None => (0, bytes.len()),
            };
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

#[tokio::test]
async fn constructor_is_lazy_send_and_redacts_token() {
    let builder = HuggingFaceBuilder::default()
        .repo_type("dataset")
        .repo_id("owner/example")
        .token(TOKEN)
        .download_mode("http");
    assert!(!format!("{builder:?}").contains(TOKEN));
    let initialize = HuggingFaceAccessor::new(builder);
    let accessor = tokio::spawn(initialize).await.unwrap().unwrap();
    assert!(!format!("{accessor:?}").contains(TOKEN));
    assert!(accessor.capability().read_with_range);
    assert!(accessor.capability().list);
    let error = HuggingFaceAccessor::new(HuggingFaceBuilder::default().token(TOKEN))
        .await
        .err()
        .unwrap();
    assert_upstream_error(
        &error,
        ErrorKind::InvalidInput,
        opendal::ErrorKind::ConfigInvalid,
        "configure_huggingface",
    );
}

#[tokio::test]
async fn ranges_preserve_order_options_metadata_and_sequential_cursor() {
    let contents = Bytes::from_static(b"abcdefghijklmnop");
    let hub = MockHub::with_files([("data/bytes.bin".to_owned(), contents.clone())]);
    let accessor = hub
        .accessor()
        .with_chunk_size(NonZeroUsize::new(3).unwrap());
    let mut reader = accessor
        .open(
            ByteReadRequest::new("bytes.bin").with_options(ByteReadOptions {
                io_concurrency: NonZeroUsize::new(2).unwrap(),
                io_chunk_size: NonZeroUsize::new(2),
            }),
        )
        .await
        .unwrap();
    assert_eq!(reader.metadata().content_length, Some(16));
    assert_eq!(
        reader.metadata().source.as_ref().unwrap().locator(),
        "bytes.bin"
    );
    assert!(!reader.capability().list);
    let ranges = reader
        .read_ranges(vec![8..10, 0..2, 8..10, 4..4])
        .await
        .unwrap();
    assert_eq!(
        ranges.iter().map(|v| v.as_ref()).collect::<Vec<_>>(),
        [b"ij".as_ref(), b"ab", b"ij", b""]
    );
    let requests = hub.captured();
    let fetched = requests
        .iter()
        .filter_map(|r| r.range.as_deref())
        .collect::<Vec<_>>();
    assert_eq!(fetched.len(), 2);
    assert!(fetched.contains(&"bytes=0-1"));
    assert!(fetched.contains(&"bytes=8-9"));
    assert!(requests.iter().all(|r| !r.uri.contains(TOKEN)));
    assert!(
        requests
            .iter()
            .any(|r| r.uri.contains("/paths-info/refs%2Fconvert%2Fparquet")
                && r.form.contains("paths=data/bytes.bin"))
    );
    let before_split = hub.captured().len();
    assert_eq!(reader.read_range(0..8).await.unwrap().as_ref(), b"abcdefgh");
    let split_requests = hub.captured();
    let split_ranges = split_requests[before_split..]
        .iter()
        .filter_map(|request| request.range.as_deref())
        .collect::<Vec<_>>();
    assert_eq!(split_ranges.len(), 4);
    for expected in ["bytes=0-1", "bytes=2-3", "bytes=4-5", "bytes=6-7"] {
        assert!(split_ranges.contains(&expected));
    }
    assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), b"abc");
    let before = hub.captured().len();
    assert_eq!(reader.read_range(16..16).await.unwrap().len(), 0);
    for range in [16..17, std::ops::Range { start: 2, end: 1 }] {
        let error = reader.read_range(range).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.operation(), "read_range");
        assert!(error.source().is_none());
    }
    let error = reader.read_ranges(vec![0..2, 16..17]).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.source().is_none());
    assert_eq!(hub.captured().len(), before);
    let chunks = open_arrow_access_core::into_stream(reader)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(chunks.concat(), contents[3..]);
    assert!(chunks.iter().all(|c| c.len() <= 3));
}

#[tokio::test]
async fn sequential_output_chunks_reuse_io_requests_and_leave_ranges_independent() {
    let bytes = Bytes::from_static(b"abcdefghijklmnop");
    let hub = MockHub::with_files([("data/bytes.bin".to_owned(), bytes.clone())]);
    let accessor = hub.accessor().with_chunk_size(NonZeroUsize::MIN);
    let mut reader = accessor
        .open(
            ByteReadRequest::new("bytes.bin").with_options(ByteReadOptions {
                io_chunk_size: NonZeroUsize::new(4),
                ..ByteReadOptions::default()
            }),
        )
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), b"a");
    assert_eq!(reader.read_range(12..14).await.unwrap().as_ref(), b"mn");
    let mut output = b"a".to_vec();
    while let Some(chunk) = reader.next().await.unwrap() {
        assert_eq!(chunk.len(), 1);
        output.extend_from_slice(&chunk);
    }
    assert_eq!(output, bytes);
    let requests = hub.captured();
    let ranges: Vec<_> = requests.iter().filter_map(|r| r.range.as_deref()).collect();
    assert_eq!(
        ranges.len(),
        5,
        "four sequential requests plus the independent range"
    );
    for range in [
        "bytes=0-3",
        "bytes=4-7",
        "bytes=8-11",
        "bytes=12-15",
        "bytes=12-13",
    ] {
        assert!(ranges.contains(&range));
    }
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(hub.captured().len(), requests.len());
}

#[tokio::test]
async fn dropping_a_sequential_stream_stops_prefetch() {
    let hub = MockHub::with_files([("data/bytes.bin".to_owned(), Bytes::from(vec![b'a'; 128]))]);
    let accessor = hub.accessor().with_chunk_size(NonZeroUsize::MIN);
    let mut reader = accessor
        .open(
            ByteReadRequest::new("bytes.bin").with_options(ByteReadOptions {
                io_chunk_size: NonZeroUsize::new(4),
                io_concurrency: NonZeroUsize::new(4).unwrap(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(hub.captured().len(), 1, "opening only inspects metadata");
    assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), b"a");
    let before = hub.captured().len();
    assert_lt!(before, 32);
    drop(reader);
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert_eq!(hub.captured().len(), before);
}

#[tokio::test]
async fn listing_recurses_and_follows_hub_pagination_without_exposing_directories() {
    let hub = MockHub::with_files([
        ("data/a.bin".to_owned(), Bytes::from_static(b"a")),
        ("data/nested/b.bin".to_owned(), Bytes::from_static(b"b")),
    ]);
    let accessor = hub.accessor();
    let objects = accessor
        .list("".into())
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        objects.iter().map(|o| o.locator()).collect::<Vec<_>>(),
        ["a.bin", "nested/b.bin"]
    );
    let requests = hub.captured();
    assert!(requests.iter().any(|r| r.uri.contains("recursive=True")));
    assert!(requests.iter().any(|r| r.uri.contains("cursor=second")));
    let error = accessor.list("a.bin".into()).await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
}

#[tokio::test]
async fn invalid_paths_fail_before_requests_and_empty_files_are_readable() {
    let hub = MockHub::with_files([("data/empty.bin".to_owned(), Bytes::new())]);
    let accessor = hub.accessor();
    for path in [
        "",
        " bytes.bin",
        "bytes.bin ",
        "/bytes.bin",
        "./bytes.bin",
        "../bytes.bin",
        "a//b",
        "a/../b",
        "a\\b",
        "a\0b",
        "a/",
    ] {
        let error = accessor.open(path.into()).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "{path:?}");
        assert_eq!(error.operation(), "open");
        assert!(error.source().is_none());
    }
    for path in ["/", "/a", "a//", "./", "../", " a/"] {
        let error = accessor.list(path.into()).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "{path:?}");
        assert_eq!(error.operation(), "list");
        assert!(error.source().is_none());
    }
    assert!(hub.captured().is_empty());
    let mut reader = accessor.open("empty.bin".into()).await.unwrap();
    assert_eq!(reader.metadata().content_length, Some(0));
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.read_range(0..0).await.unwrap().is_empty());
    assert_eq!(hub.captured().len(), 1);
}

#[tokio::test]
async fn missing_denied_and_transport_errors_retain_kind_and_terminate() {
    let hub = MockHub::with_files([("data/fail.bin".to_owned(), Bytes::from_static(b"abcdefgh"))]);
    let accessor = hub
        .accessor()
        .with_chunk_size(NonZeroUsize::new(4).unwrap());
    let error = accessor.open("missing.bin".into()).await.err().unwrap();
    assert_upstream_error(
        &error,
        ErrorKind::NotFound,
        opendal::ErrorKind::NotFound,
        "stat",
    );
    let error = accessor.open("denied.bin".into()).await.err().unwrap();
    assert_upstream_error(
        &error,
        ErrorKind::PermissionDenied,
        opendal::ErrorKind::PermissionDenied,
        "stat",
    );
    let mut reader = accessor
        .open(
            ByteReadRequest::new("fail.bin").with_options(ByteReadOptions {
                io_chunk_size: NonZeroUsize::new(4),
                ..ByteReadOptions::default()
            }),
        )
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), b"abcd");
    let error = reader.next().await.err().unwrap();
    assert_upstream_error(
        &error,
        ErrorKind::Io,
        opendal::ErrorKind::Unexpected,
        "read",
    );
    let before = hub.captured().len();
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(hub.captured().len(), before);
}

#[tokio::test]
async fn parquet_reads_through_the_hub_sdk_and_shared_range_adapter() {
    use arrow_array::Int64Array;
    use open_arrow_access_file_format::ParquetAccessor;

    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../testdata/parquet/ids.parquet");
    let bytes = Bytes::from(std::fs::read(path).unwrap());
    let hub = MockHub::with_files([("data/part.parquet".to_owned(), bytes)]);
    let accessor = ParquetAccessor::new(hub.accessor());
    let mut reader = accessor
        .open(ReadRequest::new("part.parquet").with_options(ReadOptions {
            batch_size: NonZeroUsize::new(2).unwrap(),
            projection: None,
        }))
        .await
        .unwrap();
    let mut values = Vec::<i64>::new();
    while let Some(batch) = reader.next().await.unwrap() {
        values.extend(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
        );
    }
    assert_eq!(values, [1, 2, 3]);
    assert!(hub.captured().iter().any(|r| r.range.is_some()));
}

fn assert_upstream_error(
    error: &Error,
    expected: ErrorKind,
    upstream: opendal::ErrorKind,
    operation: &str,
) {
    assert_eq!(error.kind(), expected);
    assert_eq!(error.operation(), operation);
    assert_eq!(ErrorKind::from_source(error), Some(expected));
    assert_eq!(
        error
            .source()
            .unwrap()
            .downcast_ref::<opendal::Error>()
            .unwrap()
            .kind(),
        upstream,
    );
    assert!(!format!("{error:?}").contains(TOKEN));
}

#[tokio::test]
async fn metadata_http_status_codes_preserve_classification_and_source() {
    for (status, expected, upstream) in [
        (
            StatusCode::NOT_FOUND,
            ErrorKind::NotFound,
            opendal::ErrorKind::NotFound,
        ),
        (
            StatusCode::UNAUTHORIZED,
            ErrorKind::PermissionDenied,
            opendal::ErrorKind::PermissionDenied,
        ),
        (
            StatusCode::FORBIDDEN,
            ErrorKind::PermissionDenied,
            opendal::ErrorKind::PermissionDenied,
        ),
        (
            StatusCode::TOO_MANY_REQUESTS,
            ErrorKind::Io,
            opendal::ErrorKind::RateLimited,
        ),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorKind::Io,
            opendal::ErrorKind::Unexpected,
        ),
    ] {
        let hub = MockHub::default().with_failure("/paths-info/", HubFailure::Status(status));
        let accessor = hub.accessor();
        let error = accessor.open("file.bin".into()).await.err().unwrap();
        assert_upstream_error(&error, expected, upstream, "stat");
        let error = accessor.list("nested".into()).await.err().unwrap();
        assert_upstream_error(&error, expected, upstream, "list");
        assert!(
            hub.captured()
                .iter()
                .all(|request| request.uri.contains("/paths-info/"))
        );
    }
}

#[tokio::test]
async fn read_http_status_codes_preserve_source_and_stop_sequential_reads() {
    for (status, expected, upstream) in [
        (
            StatusCode::NOT_FOUND,
            ErrorKind::NotFound,
            opendal::ErrorKind::NotFound,
        ),
        (
            StatusCode::UNAUTHORIZED,
            ErrorKind::PermissionDenied,
            opendal::ErrorKind::PermissionDenied,
        ),
        (
            StatusCode::FORBIDDEN,
            ErrorKind::PermissionDenied,
            opendal::ErrorKind::PermissionDenied,
        ),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorKind::Io,
            opendal::ErrorKind::Unexpected,
        ),
    ] {
        let hub = MockHub::with_files([("data/file.bin".into(), Bytes::from_static(b"abcdefgh"))])
            .with_failure("/resolve/", HubFailure::Status(status));
        let accessor = hub.accessor();
        let mut reader = accessor.open("file.bin".into()).await.unwrap();
        let error = reader.read_range(0..2).await.unwrap_err();
        assert_upstream_error(&error, expected, upstream, "read");
        let error = reader.read_ranges(vec![0..1, 4..5]).await.unwrap_err();
        assert_upstream_error(&error, expected, upstream, "read_ranges");
        let error = reader.next().await.unwrap_err();
        assert_upstream_error(&error, expected, upstream, "read");
        let calls = hub.captured().len();
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(hub.captured().len(), calls);
    }
}

#[tokio::test]
async fn transport_and_unsupported_failures_retain_original_categories() {
    for (upstream, expected) in [
        (opendal::ErrorKind::Unexpected, ErrorKind::Io),
        (opendal::ErrorKind::Unsupported, ErrorKind::Unsupported),
    ] {
        let hub = MockHub::with_files([("data/file.bin".into(), Bytes::from_static(b"payload"))])
            .with_failure("/resolve/", HubFailure::Transport(upstream));
        let mut reader = hub.accessor().open("file.bin".into()).await.unwrap();
        let error = reader.next().await.unwrap_err();
        assert_upstream_error(&error, expected, upstream, "read");
        let calls = hub.captured().len();
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(hub.captured().len(), calls);
    }
}

#[tokio::test]
async fn listing_http_failures_preserve_categories_and_sources() {
    for (status, expected, upstream) in [
        (
            StatusCode::FORBIDDEN,
            ErrorKind::PermissionDenied,
            opendal::ErrorKind::PermissionDenied,
        ),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorKind::Io,
            opendal::ErrorKind::Unexpected,
        ),
    ] {
        let hub = MockHub::default().with_failure("/tree/", HubFailure::Status(status));
        let mut objects = hub.accessor().list("".into()).await.unwrap();
        let error = objects.try_next().await.unwrap_err();
        assert_upstream_error(&error, expected, upstream, "list");
        let calls = hub.captured().len();
        assert!(objects.try_next().await.unwrap().is_none());
        assert_eq!(hub.captured().len(), calls);
    }
}

use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use futures::{TryStreamExt, stream};
use http::{Request, Response, StatusCode};
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadOptions, ByteReadRequest, Bytes, ErrorKind, ReadOptions,
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

#[derive(Clone, Default)]
struct MockHub {
    files: Arc<BTreeMap<String, Bytes>>,
    requests: Arc<Mutex<Vec<Captured>>>,
}

impl MockHub {
    fn with_files(files: impl IntoIterator<Item = (String, Bytes)>) -> Self {
        Self {
            files: Arc::new(files.into_iter().collect()),
            ..Self::default()
        }
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
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(!format!("{error:?}").contains(TOKEN));
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
    assert_eq!(
        reader.read_range(16..17).await.err().unwrap().kind(),
        ErrorKind::InvalidInput
    );
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
    assert!(before < 32);
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
        assert_eq!(
            accessor.open(path.into()).await.err().unwrap().kind(),
            ErrorKind::InvalidInput,
            "{path:?}"
        );
    }
    for path in ["/", "/a", "a//", "./", "../", " a/"] {
        assert_eq!(
            accessor.list(path.into()).await.err().unwrap().kind(),
            ErrorKind::InvalidInput,
            "{path:?}"
        );
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
    assert_eq!(
        accessor
            .open("missing.bin".into())
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::NotFound
    );
    assert_eq!(
        accessor
            .open("denied.bin".into())
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::PermissionDenied
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
    assert_eq!(error.kind(), ErrorKind::Io);
    assert!(!format!("{error:?}").contains(TOKEN));
    let before = hub.captured().len();
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(hub.captured().len(), before);
}

#[tokio::test]
async fn parquet_reads_through_the_hub_sdk_and_shared_range_adapter() {
    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use open_arrow_access_file_format::ParquetAccessor;
    use parquet::arrow::ArrowWriter;
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    let bytes = Bytes::from(writer.into_inner().unwrap());
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

#![cfg(feature = "service-commoncrawl")]

use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use arrow_array::{Array, StringArray};
use arrow_schema::{DataType, Field};
use futures::{TryStreamExt, stream};
use http::{Request, Response, StatusCode};
use open_arrow_access::{
    Accessor, ByteAccessor, ByteMetadata, ByteReadOptions, ByteReadRequest, ByteReader, Bytes,
    Capability, Error, ErrorKind, ReadOptions, ReadOptionsProvider, ReadRequest, Reader,
    RecordBatch, RecordMetadata, RecordReader, Result, Schema, async_trait, into_stream,
    service::{CommonCrawlSegmentAccessor, CommonCrawlSegmentRequest, create_http_accessor},
    storage::{OpenDalAccessor, opendal},
};
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter, OperationContext, Operator};

const PREFIX: &str = "crawl-data/CC-MAIN-2022-40/segments/1664030338280.51/warc/";
const MANIFEST: &str = "crawl-data/CC-MAIN-2022-40/warc.paths.gz";

fn request() -> CommonCrawlSegmentRequest {
    CommonCrawlSegmentRequest::new("CC-MAIN-2022-40", "1664030338280.51").unwrap()
}

fn entry(name: &str) -> String {
    format!("{PREFIX}{name}.warc.gz")
}

#[derive(Clone)]
struct Manifest {
    chunks: Vec<Bytes>,
    fail_after: Option<usize>,
    opens: Arc<Mutex<Vec<ByteReadRequest>>>,
}

impl Manifest {
    fn fragmented(bytes: impl Into<Bytes>, size: usize) -> Self {
        let bytes: Bytes = bytes.into();
        Self {
            chunks: bytes.chunks(size).map(Bytes::copy_from_slice).collect(),
            fail_after: None,
            opens: Arc::default(),
        }
    }
}

#[async_trait]
impl Accessor for Manifest {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            ..Capability::default()
        }
    }
    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        self.opens.lock().unwrap().push(request.clone());
        Ok(Box::new(ManifestReader {
            chunks: self.chunks.clone().into_iter(),
            fail_after: self.fail_after,
            index: 0,
            metadata: ByteMetadata {
                source: Some(request.object),
                ..ByteMetadata::default()
            },
        }))
    }
}
#[async_trait]
impl ByteAccessor for Manifest {}

struct ManifestReader {
    chunks: std::vec::IntoIter<Bytes>,
    fail_after: Option<usize>,
    index: usize,
    metadata: ByteMetadata,
}
#[async_trait]
impl Reader for ManifestReader {
    type Item = Bytes;
    type Metadata = ByteMetadata;
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            ..Capability::default()
        }
    }
    fn metadata(&self) -> &ByteMetadata {
        &self.metadata
    }
    async fn next(&mut self) -> Result<Option<Bytes>> {
        if self.fail_after == Some(self.index) {
            self.fail_after = None;
            self.chunks = Vec::new().into_iter();
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "fixture_manifest",
                "denied",
            ));
        }
        self.index += 1;
        Ok(self.chunks.next())
    }
}
#[async_trait]
impl ByteReader for ManifestReader {}

#[derive(Clone, Default)]
struct Records {
    opens: Arc<Mutex<Vec<ReadRequest>>>,
}

#[async_trait]
impl Accessor for Records {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            ..Capability::default()
        }
    }
    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        self.opens.lock().unwrap().push(request.clone());
        if request.object.locator().ends_with("bad.warc.gz") {
            return Err(Error::new(ErrorKind::Io, "fixture_records", "input failed"));
        }
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("path", DataType::Utf8, false)],
            HashMap::from([("fixture".into(), "provider".into())]),
        ));
        let mut batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![request.object.locator()]))],
        )
        .unwrap();
        if let Some(columns) = &request.options.projection {
            let projection = columns
                .iter()
                .map(|name| batch.schema().index_of(name).unwrap())
                .collect::<Vec<_>>();
            batch = batch.project(&projection).unwrap();
        }
        Ok(Box::new(Batches {
            metadata: RecordMetadata {
                schema: batch.schema(),
                source: Some(request.object),
            },
            batch: Some(batch),
        }))
    }
}
struct Batches {
    metadata: RecordMetadata,
    batch: Option<RecordBatch>,
}
#[async_trait]
impl Reader for Batches {
    type Item = RecordBatch;
    type Metadata = RecordMetadata;
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            ..Capability::default()
        }
    }
    fn metadata(&self) -> &RecordMetadata {
        &self.metadata
    }
    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.batch.take())
    }
}

#[test]
fn segment_selection_accepts_example_and_rejects_url_aliases() {
    let selection = CommonCrawlSegmentRequest::from_url(
        "https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-40/segments/1664030338280.51/",
    )
    .unwrap();
    assert_eq!(selection.crawl(), "CC-MAIN-2022-40");
    assert_eq!(selection.segment(), "1664030338280.51");
    assert_eq!(selection.manifest().locator(), MANIFEST);
    assert_eq!(selection.origin(), "https://data.commoncrawl.org");
    for value in [
        "https://host/crawl-data/CC-MAIN-2022-40/segments/1664030338280.51/?token=x",
        "https://user:pass@host/crawl-data/CC-MAIN-2022-40/segments/1664030338280.51/",
        "https://host/crawl-data/CC-MAIN-2022-40/x/../segments/1664030338280.51/",
        "https://host/crawl-data/CC-MAIN-2022-40/segments/%31.2/",
        "https://host/crawl-data/CC-MAIN-2022-40/segments/1664030338280.51//",
        "https://host/crawl-data/CC-MAIN-2022-40//segments/1664030338280.51/",
        "file:///crawl-data/CC-MAIN-2022-40/segments/1664030338280.51/",
    ] {
        assert_eq!(
            CommonCrawlSegmentRequest::from_url(value)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput,
            "{value}"
        );
    }
    for (crawl, segment) in [
        ("../CC-MAIN-2022-40", "1.2"),
        ("CC-MAIN-2022-40", "../1.2"),
        ("CC-MAIN-2022-40", "1.2.3"),
    ] {
        assert!(CommonCrawlSegmentRequest::new(crawl, segment).is_err());
    }
}

#[tokio::test]
async fn manifest_resolves_exact_segment_in_original_order_with_forwarded_options() {
    let text = format!(
        "{}\r\n\ncrawl-data/CC-MAIN-2022-40/segments/1664030338280.510/warc/other.warc.gz\n{}",
        entry("z"),
        entry("a")
    );
    let manifests = Manifest::fragmented(text, 1);
    let records = Records::default();
    let opens = records.opens.clone();
    let manifest_opens = manifests.opens.clone();
    let accessor = CommonCrawlSegmentAccessor::new(manifests, records);
    assert!(accessor.capability().read);
    assert!(!accessor.capability().read_with_range);
    assert!(!accessor.capability().list);
    let mut selection = request().with_byte_options(ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        io_chunk_size: NonZeroUsize::new(4),
    });
    selection.read_options_mut().batch_size = NonZeroUsize::new(2).unwrap();
    let reader = accessor.open(selection).await.unwrap();
    assert_eq!(opens.lock().unwrap().len(), 1);
    assert!(reader.metadata().source.is_none());
    assert_eq!(reader.metadata().schema.metadata()["fixture"], "provider");
    let batches = into_stream(reader).try_collect::<Vec<_>>().await.unwrap();
    let paths = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|x| x.unwrap().to_owned())
        })
        .collect::<Vec<_>>();
    assert_eq!(paths, [entry("z"), entry("a")]);
    let manifest_opens = manifest_opens.lock().unwrap();
    assert_eq!(manifest_opens[0].object.locator(), MANIFEST);
    assert_eq!(manifest_opens[0].options.io_concurrency.get(), 3);
    let opens = opens.lock().unwrap();
    assert_eq!(opens.len(), 2);
    assert!(
        opens
            .iter()
            .all(|r| r.options.batch_size.get() == 2 && r.byte_options.io_concurrency.get() == 3)
    );
}

#[tokio::test]
async fn projection_and_final_reader_errors_keep_shared_contracts() {
    let text = format!("{}\n{}\n{}\n", entry("first"), entry("bad"), entry("last"));
    let records = Records::default();
    let opens = records.opens.clone();
    let accessor = CommonCrawlSegmentAccessor::new(Manifest::fragmented(text, 7), records);
    let mut reader = accessor
        .open(request().with_options(ReadOptions {
            projection: Some(vec![]),
            ..ReadOptions::default()
        }))
        .await
        .unwrap();
    assert_eq!(reader.metadata().schema.fields().len(), 0);
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 0);
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Io);
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(opens.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn malformed_alias_duplicate_and_oversized_manifest_lines_fail_before_file_opens() {
    let invalid = [
        format!("{}\n{}\n", entry("a"), entry("a")),
        format!("{PREFIX}../warc/a.warc.gz\n"),
        format!(" {PREFIX}a.warc.gz\n"),
        format!("/{PREFIX}a.warc.gz\n"),
        format!("https://evil.invalid/{PREFIX}a.warc.gz\n"),
        format!("{PREFIX}a.warc.gz?query=1\n"),
        format!("{PREFIX}%61.warc.gz\n"),
        format!("{PREFIX}a.txt\n"),
        "x".repeat(16 * 1024 + 1),
    ];
    for text in invalid {
        let records = Records::default();
        let opens = records.opens.clone();
        let accessor = CommonCrawlSegmentAccessor::new(Manifest::fragmented(text, 19), records);
        assert_eq!(
            accessor.open(request()).await.err().unwrap().kind(),
            ErrorKind::Decode
        );
        assert!(opens.lock().unwrap().is_empty());
    }
    let accessor = CommonCrawlSegmentAccessor::new(
        Manifest::fragmented(Bytes::from_static(b"\xff\n"), 1),
        Records::default(),
    );
    assert_eq!(
        accessor.resolve(request()).await.unwrap_err().kind(),
        ErrorKind::Decode
    );
}

#[tokio::test]
async fn empty_no_match_raw_gzip_and_upstream_errors_are_explicit() {
    for text in [
        String::new(),
        "\n\r\n".to_owned(),
        "crawl-data/CC-MAIN-2022-40/segments/1664030338280.510/warc/x.warc.gz\n".to_owned(),
    ] {
        let accessor =
            CommonCrawlSegmentAccessor::new(Manifest::fragmented(text, 2), Records::default());
        assert_eq!(
            accessor.resolve(request()).await.unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
    let mut manifests = Manifest::fragmented(entry("a"), 3);
    manifests.fail_after = Some(1);
    let accessor = CommonCrawlSegmentAccessor::new(manifests, Records::default());
    assert_eq!(
        accessor.resolve(request()).await.unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
    let accessor = CommonCrawlSegmentAccessor::new(
        Manifest::fragmented(Bytes::from_static(b"\x1f\x8b\x08\x00"), 1),
        Records::default(),
    );
    assert_eq!(
        accessor.resolve(request()).await.unwrap_err().kind(),
        ErrorKind::Decode
    );
}

#[test]
fn http_configuration_is_lazy_and_has_truthful_capabilities() {
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
        assert!(create_http_accessor(origin).is_err(), "{origin}");
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
async fn public_opendal_adapter_reads_manifest_from_origin_root_through_injected_http() {
    let transport = MockHttp {
        manifest: Bytes::from(format!("{}\n", entry("a"))),
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
    assert!(!bytes.capability().list);
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
    let accessor = CommonCrawlSegmentAccessor::new(bytes, Records::default());
    let resolved = accessor.resolve(request()).await.unwrap();
    assert_eq!(resolved.objects, vec![entry("a").into()]);
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

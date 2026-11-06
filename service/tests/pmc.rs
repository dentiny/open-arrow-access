use arrow_array::{Array, LargeBinaryArray, LargeStringArray, StringArray, UInt32Array};
use futures::{StreamExt, stream};
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteAccessor, ByteReadOptions, ByteReadRequest, ByteReader, Capability,
    Error, ErrorKind, ListRequest, ObjectRef, ObjectStream, ReadOptions, ReadOptionsProvider,
    RecordBatch, RecordReader, Result, async_trait,
};
use open_arrow_access_core::{ByteMetadata, Bytes, Reader};
use open_arrow_access_decompression::{DecompressionLayer, Gzip};
use open_arrow_access_service::pmc::{PmcAccessor, PmcInput, PmcLimits, PmcRequest, schema};
use open_arrow_access_storage::FsAccessor;
use open_arrow_access_test_support::MemoryAccessor;
use std::{
    collections::HashMap,
    error::Error as _,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

const RAW: &str = include_str!("../../testdata/pmc/metadata.json");
const PDF: &[u8] = include_bytes!("../../testdata/pmc/article.pdf");
const PDF_PATH: &str = "PMC123.2/article +%20.pdf";

#[derive(Clone, Default)]
struct Objects {
    files: HashMap<String, MemoryAccessor>,
    listing: Vec<ObjectRef>,
    opened: Arc<Mutex<Vec<ByteReadRequest>>>,
    listed: Arc<Mutex<Vec<ListRequest>>>,
    listing_polls: Arc<AtomicUsize>,
    listing_error: bool,
}

impl Objects {
    fn insert(&mut self, path: &str, bytes: &[u8]) {
        self.files
            .insert(path.into(), MemoryAccessor::new(bytes, 1));
    }
    fn fixture() -> Self {
        let mut source = Self::default();
        source.insert("metadata/a.json", RAW.as_bytes());
        source.insert(PDF_PATH, PDF);
        source
    }
}

#[async_trait]
impl Accessor for Objects {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            list: true,
            ..Capability::default()
        }
    }
    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        self.opened.lock().unwrap().push(request.clone());
        self.files
            .get(request.object.locator())
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "fixture", "object missing"))?
            .open(request)
            .await
    }
}

#[async_trait]
impl ByteAccessor for Objects {
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        self.listed.lock().unwrap().push(request);
        let mut objects: Vec<_> = self.listing.iter().cloned().map(Ok).collect();
        if self.listing_error {
            objects.push(Err(Error::new(
                ErrorKind::PermissionDenied,
                "fixture_list",
                "listing denied",
            )));
        }
        let polls = self.listing_polls.clone();
        Ok(Box::pin(stream::iter(objects).inspect(move |_| {
            polls.fetch_add(1, Ordering::SeqCst);
        })))
    }
}

fn options(rows: usize, projection: Option<Vec<&str>>) -> ReadOptions {
    ReadOptions {
        batch_size: NonZeroUsize::new(rows).unwrap(),
        projection: projection.map(|names| names.into_iter().map(str::to_owned).collect()),
    }
}

async fn all(mut reader: Box<dyn RecordReader>) -> Vec<RecordBatch> {
    let schema = reader.metadata().schema.clone();
    let mut batches = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.schema(), schema);
        batches.push(batch);
    }
    assert!(reader.next().await.unwrap().is_none());
    batches
}

async fn error(source: Objects, request: PmcRequest) -> Error {
    let mut reader = PmcAccessor::new(source).open(request).await.unwrap();
    let error = reader.next().await.unwrap_err();
    assert!(reader.next().await.unwrap().is_none());
    error
}

#[tokio::test]
async fn maps_metadata_and_preserves_verbatim_pdf_json_and_provenance() {
    let source = Objects::fixture();
    let io = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        io_chunk_size: NonZeroUsize::new(11),
    };
    let reader = PmcAccessor::new(source.clone())
        .open(PmcRequest::new("metadata/a.json").with_byte_options(io))
        .await
        .unwrap();
    assert!(source.opened.lock().unwrap().is_empty());
    assert_eq!(reader.metadata().schema, schema());
    assert!(!reader.capability().read_parallel);
    let batches = all(reader).await;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "PMC123"
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .value(0),
        2
    );
    for (index, value) in [
        (2, "36537061"),
        (3, "10.0000/synthetic"),
        (5, "CC BY"),
        (
            6,
            "s3://pmc-oa-opendata/PMC123.2/article%20%2B%2520.pdf?md5=example",
        ),
    ] {
        assert_eq!(
            batch
                .column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            value
        );
    }
    assert_eq!(
        batch
            .column(4)
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .value(0),
        "Synthetic 医学 article"
    );
    assert_eq!(
        batch
            .column(7)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .value(0),
        PDF
    );
    assert_eq!(
        batch
            .column(8)
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .value(0),
        RAW
    );
    assert_eq!(
        batch.schema().metadata()["pmc_arrow.row_kind"],
        "pdf_snapshot"
    );
    let opened = source.opened.lock().unwrap();
    assert_eq!(opened.len(), 2);
    assert_eq!(opened[1].object.locator(), PDF_PATH);
    assert!(opened.iter().all(|request| request.options == io));
}

#[tokio::test]
async fn batches_in_input_order_retains_duplicates_and_flushes_partial_batch() {
    let mut source = Objects::fixture();
    source.insert(
        "metadata/b.json",
        br#"{"pmcid":"456","pmid":"00042","pdf_url":"s3://pmc-oa-opendata/b.pdf"}"#,
    );
    source.insert("b.pdf", PDF);
    let request = PmcRequest::files(["metadata/b.json", "metadata/a.json", "metadata/b.json"])
        .with_options(options(2, None));
    let batches = all(PmcAccessor::new(source).open(request).await.unwrap()).await;
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.num_rows())
            .collect::<Vec<_>>(),
        [2, 1]
    );
    let ids: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .flatten()
        })
        .collect();
    assert_eq!(ids, ["PMC456", "PMC123", "PMC456"]);
    assert!(batches[0].column(1).is_null(0));
    assert_eq!(
        batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "00042"
    );
    assert!(batches[0].column(4).is_null(0));
}

#[tokio::test]
async fn missing_pdf_skips_only_that_article_and_empty_inputs_keep_schema() {
    let mut source = Objects::fixture();
    source.insert(
        "metadata/none.json",
        br#"{"pmcid":false,"pdf_url":null,"unknown":1e9999}"#,
    );
    source.insert("metadata/absent.json", br#"{"pmcid":"PMC1"}"#);
    for files in [vec![], vec!["metadata/none.json", "metadata/absent.json"]] {
        let reader = PmcAccessor::new(source.clone())
            .open(PmcRequest::files(files))
            .await
            .unwrap();
        assert_eq!(reader.metadata().schema, schema());
        assert!(all(reader).await.is_empty());
    }
    let batches = all(PmcAccessor::new(source.clone())
        .open(PmcRequest::files(["metadata/none.json", "metadata/a.json"]))
        .await
        .unwrap())
    .await;
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(
        source
            .opened
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.object.locator() == PDF_PATH)
            .count(),
        1
    );
}

#[tokio::test]
async fn final_projection_preserves_schema_order_and_zero_column_row_counts() {
    for names in [vec!["pdf_bytes", "pmcid"], vec!["title"], vec![]] {
        let source = PmcAccessor::new(Objects::fixture());
        let request = PmcRequest::files(["metadata/a.json", "metadata/a.json"])
            .with_options(options(2, Some(names.clone())));
        let reader = source.open(request).await.unwrap();
        let expected = reader.metadata().schema.clone();
        let batches = all(reader).await;
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].num_columns(), names.len());
        assert_eq!(batches[0].schema(), expected);
        assert_eq!(
            expected
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            names
        );
        assert_eq!(expected.metadata(), schema().metadata());
    }
    let source = Objects::fixture();
    for names in [vec!["unknown"], vec!["pmcid", "pmcid"]] {
        let mut request = PmcRequest::prefix("metadata/");
        *request.read_options_mut() = options(1, Some(names));
        assert_eq!(request.read_options().batch_size.get(), 1);
        let result = PmcAccessor::new(source.clone()).open(request).await;
        assert_eq!(result.err().unwrap().kind(), ErrorKind::InvalidInput);
    }
    assert!(source.listed.lock().unwrap().is_empty());
    assert!(source.opened.lock().unwrap().is_empty());
}

#[tokio::test]
async fn discovery_is_lazy_filters_files_and_stops_on_drop() {
    let mut source = Objects::fixture();
    source.listing = [
        "metadata/",
        "metadata/readme.txt",
        "metadata/a.json",
        "metadata/a.json",
    ]
    .into_iter()
    .map(Into::into)
    .collect();
    let accessor = PmcAccessor::new(source.clone());
    let mut reader = accessor
        .open(PmcRequest::prefix("metadata").with_options(options(1, None)))
        .await
        .unwrap();
    assert_eq!(source.listing_polls.load(Ordering::SeqCst), 0);
    assert!(source.opened.lock().unwrap().is_empty());
    assert_eq!(
        source.listed.lock().unwrap()[0].prefix.locator(),
        "metadata/"
    );
    assert!(source.listed.lock().unwrap()[0].recursive);
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
    assert_eq!(source.listing_polls.load(Ordering::SeqCst), 3);
    drop(reader);
    assert_eq!(source.opened.lock().unwrap().len(), 2);
    assert_eq!(source.listing_polls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn discovery_errors_are_terminal_and_recursion_option_is_forwarded() {
    let mut source = Objects::fixture();
    source.listing_error = true;
    let mut request = PmcRequest::prefix("metadata/");
    if let PmcInput::Prefix(list) = &mut request.input {
        list.recursive = false;
    }
    assert_eq!(
        error(source.clone(), request).await.kind(),
        ErrorKind::PermissionDenied
    );
    assert!(!source.listed.lock().unwrap()[0].recursive);
    for path in [
        "elsewhere/a.json",
        "metadata/../a.json",
        "metadata//a.json",
        "metadata/a\\b.json",
        "metadata2/a.json",
    ] {
        source.listing_error = false;
        source.listing = vec![path.into()];
        assert_eq!(
            error(source.clone(), PmcRequest::prefix("metadata/"))
                .await
                .kind(),
            ErrorKind::InvalidInput
        );
    }
}

#[tokio::test]
async fn unsupported_listing_accepts_explicit_files() {
    let source = PmcAccessor::new(MemoryAccessor::new(b"{}", 1));
    let result = source.open(PmcRequest::prefix("metadata/")).await;
    assert_eq!(result.err().unwrap().kind(), ErrorKind::Unsupported);
    assert!(
        all(source.open(PmcRequest::new("a.json")).await.unwrap())
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn payload_target_flushes_after_each_bounded_article() {
    let mut request = PmcRequest::files(["metadata/a.json", "metadata/a.json"]);
    request.limits.batch_bytes = NonZeroUsize::MIN;
    let batches = all(PmcAccessor::new(Objects::fixture())
        .open(request)
        .await
        .unwrap())
    .await;
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.num_rows())
            .collect::<Vec<_>>(),
        [1, 1]
    );
}

#[tokio::test]
async fn hard_object_limits_reject_metadata_and_pdf() {
    for pdf in [false, true] {
        let mut limits = PmcLimits::default();
        if pdf {
            limits.pdf_bytes = NonZeroUsize::new(PDF.len() - 1).unwrap();
        } else {
            limits.metadata_bytes = NonZeroUsize::new(RAW.len() - 1).unwrap();
        }
        let source = Objects::fixture();
        let failure = error(
            source.clone(),
            PmcRequest::new("metadata/a.json").with_limits(limits),
        )
        .await;
        assert_eq!(failure.kind(), ErrorKind::Decode);
        assert!(failure.to_string().contains("object limit"));
        assert_eq!(source.opened.lock().unwrap().len(), if pdf { 2 } else { 1 });
    }
}

#[tokio::test]
async fn malformed_metadata_and_invalid_mapped_values_fail_without_pdf_io() {
    for raw in [
        "[]",
        "null",
        "42",
        "{",
        "{} {}",
        "{\"pmcid\":\"PMC1\"} trailing",
        r#"{"pdf_url":false}"#,
        r#"{"pdf_url":"s3://pmc-oa-opendata/a.pdf"}"#,
        r#"{"pmcid":"PMC0","pdf_url":"s3://pmc-oa-opendata/a.pdf"}"#,
        r#"{"pmcid":"PMC1.2","pdf_url":"s3://pmc-oa-opendata/a.pdf"}"#,
        r#"{"pmcid":"PMC1","version":0,"pdf_url":"s3://pmc-oa-opendata/a.pdf"}"#,
        r#"{"pmcid":"PMC1","version":4294967296,"pdf_url":"s3://pmc-oa-opendata/a.pdf"}"#,
        r#"{"pmcid":"PMC1","version":"1","pdf_url":"s3://pmc-oa-opendata/a.pdf"}"#,
        r#"{"pmcid":"PMC1","pmid":1.5,"pdf_url":"s3://pmc-oa-opendata/a.pdf"}"#,
        r#"{"pmcid":"PMC1","pmid":false,"pdf_url":"s3://pmc-oa-opendata/a.pdf"}"#,
        r#"{"pmcid":"PMC1","title":23,"pdf_url":"s3://pmc-oa-opendata/a.pdf"}"#,
    ] {
        let mut source = Objects::fixture();
        source.insert("metadata/a.json", raw.as_bytes());
        let failure = error(source.clone(), PmcRequest::new("metadata/a.json")).await;
        assert_eq!(failure.kind(), ErrorKind::Decode, "{raw}");
        assert!(failure.to_string().contains("metadata/a.json"), "{raw}");
        assert_eq!(source.opened.lock().unwrap().len(), 1, "{raw}");
    }
    let mut source = Objects::fixture();
    source.insert("metadata/a.json", &[0xff]);
    assert_eq!(
        error(source, PmcRequest::new("metadata/a.json"))
            .await
            .kind(),
        ErrorKind::Decode
    );
}

#[tokio::test]
async fn nullable_empty_fields_and_duplicate_keys_follow_source_mapping() {
    let mut source = Objects::fixture();
    source.insert("metadata/a.json", br#"{"pmcid":"PMC1","pmid":1,"pmid":"00001","version":null,"doi":"","title":"","license_code":null,"pdf_url":"s3://pmc-oa-opendata/a.pdf"}"#);
    source.insert("a.pdf", PDF);
    let batches = all(PmcAccessor::new(source)
        .open(PmcRequest::new("metadata/a.json"))
        .await
        .unwrap())
    .await;
    let batch = &batches[0];
    assert!(batch.column(1).is_null(0));
    assert_eq!(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "00001"
    );
    assert_eq!(
        batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        ""
    );
    assert_eq!(
        batch
            .column(4)
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .value(0),
        ""
    );
    assert!(batch.column(5).is_null(0));
}

#[tokio::test]
async fn invalid_urls_cannot_escape_or_alias_bucket_objects() {
    for uri in [
        "https://example.com/a.pdf",
        "s3://other/a.pdf",
        "s3://pmc-oa-opendata/",
        "s3://pmc-oa-opendata//a.pdf",
        "s3://pmc-oa-opendata/../a.pdf",
        "s3://pmc-oa-opendata/a/%2e%2e/b.pdf",
        "s3://pmc-oa-opendata/a%5cb.pdf",
        "s3://pmc-oa-opendata/a%00b.pdf",
        "s3://pmc-oa-opendata/%FF.pdf",
        "s3://pmc-oa-opendata/a.pdf%20",
    ] {
        let mut source = Objects::fixture();
        let raw = serde_json::json!({"pmcid":"PMC1", "pdf_url":uri}).to_string();
        source.insert("metadata/a.json", raw.as_bytes());
        assert_eq!(
            error(source.clone(), PmcRequest::new("metadata/a.json"))
                .await
                .kind(),
            ErrorKind::Decode,
            "{uri}"
        );
        assert_eq!(source.opened.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn invalid_headers_and_storage_failures_are_terminal_without_partial_rows() {
    for pdf in [
        b"".as_slice(),
        b"<html>error</html>",
        b"%PDF-1.7",
        b"%PDF-x.y\n",
    ] {
        let mut source = Objects::fixture();
        source.insert(PDF_PATH, pdf);
        assert_eq!(
            error(source, PmcRequest::new("metadata/a.json"))
                .await
                .kind(),
            ErrorKind::Decode
        );
    }
    let mut source = Objects::fixture();
    source.files.remove(PDF_PATH);
    assert_eq!(
        error(source, PmcRequest::new("metadata/a.json"))
            .await
            .kind(),
        ErrorKind::NotFound
    );
    for pdf in [false, true] {
        let mut source = Objects::fixture();
        let (path, bytes) = if pdf {
            (PDF_PATH, PDF)
        } else {
            ("metadata/a.json", RAW.as_bytes())
        };
        source.files.insert(
            path.into(),
            MemoryAccessor::new(bytes, 1).with_failure(bytes.len(), ErrorKind::PermissionDenied),
        );
        let failure = error(source, PmcRequest::new("metadata/a.json")).await;
        assert_eq!(failure.kind(), ErrorKind::PermissionDenied);
        assert!(failure.source().is_some());
    }
}

#[tokio::test]
async fn independent_opens_and_local_mirror_use_existing_fs_reader() {
    let root = tempfile::tempdir().unwrap();
    tokio::fs::create_dir(root.path().join("metadata"))
        .await
        .unwrap();
    tokio::fs::create_dir(root.path().join("PMC123.2"))
        .await
        .unwrap();
    tokio::fs::write(root.path().join("metadata/a.json"), RAW)
        .await
        .unwrap();
    tokio::fs::write(root.path().join(PDF_PATH), PDF)
        .await
        .unwrap();
    let source = PmcAccessor::new(
        FsAccessor::new(root.path().to_str().unwrap())
            .await
            .unwrap(),
    );
    let mut first = source.open(PmcRequest::prefix("metadata/")).await.unwrap();
    let mut second = source
        .open(PmcRequest::new("metadata/a.json"))
        .await
        .unwrap();
    let a = first.next().await.unwrap().unwrap();
    let b = second.next().await.unwrap().unwrap();
    assert_eq!(a, b);
    assert!(first.next().await.unwrap().is_none());
    assert!(second.next().await.unwrap().is_none());
}

#[tokio::test]
async fn bad_metadata_selections_fail_before_io() {
    let source = Objects::fixture();
    for path in [
        "/a.json",
        "../a.json",
        "a/./b.json",
        "a//b.json",
        "a\\b.json",
        " a.json",
        "s3://bucket/a.json",
    ] {
        for request in [PmcRequest::new(path), PmcRequest::prefix(path)] {
            let result = PmcAccessor::new(source.clone()).open(request).await;
            assert_eq!(
                result.err().unwrap().kind(),
                ErrorKind::InvalidInput,
                "{path}"
            );
        }
    }
    assert!(source.opened.lock().unwrap().is_empty());
    assert!(source.listed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn reuses_gzip_layer_and_enforces_limits_in_decoded_byte_coordinates() {
    let mut source = Objects::fixture();
    source.insert(
        "metadata/a.json",
        include_bytes!("../../testdata/pmc/metadata.json.gz"),
    );
    source.insert(
        PDF_PATH,
        include_bytes!("../../testdata/pmc/article.pdf.gz"),
    );
    let decoded =
        source.layer(DecompressionLayer::new(Gzip).with_chunk_size(NonZeroUsize::new(3).unwrap()));
    let source = PmcAccessor::new(decoded);
    let batches = all(source
        .open(PmcRequest::new("metadata/a.json"))
        .await
        .unwrap())
    .await;
    assert_eq!(
        batches[0]
            .column(7)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .value(0),
        PDF
    );
    assert_eq!(
        batches[0]
            .column(8)
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .value(0),
        RAW
    );
    for pdf in [false, true] {
        let mut limits = PmcLimits::default();
        if pdf {
            limits.pdf_bytes = NonZeroUsize::new(PDF.len() - 1).unwrap();
        } else {
            limits.metadata_bytes = NonZeroUsize::new(RAW.len() - 1).unwrap();
        }
        let mut reader = source
            .open(PmcRequest::new("metadata/a.json").with_limits(limits))
            .await
            .unwrap();
        let failure = reader.next().await.unwrap_err();
        assert_eq!(failure.kind(), ErrorKind::Decode);
        assert!(failure.to_string().contains("object limit"));
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn empty_upstream_chunks_are_errors_instead_of_eof_or_busy_loops() {
    for pdf in [false, true] {
        let mut source = Objects::fixture();
        let (path, bytes) = if pdf {
            (PDF_PATH, PDF)
        } else {
            ("metadata/a.json", RAW.as_bytes())
        };
        source.files.insert(
            path.into(),
            MemoryAccessor::new(bytes, 1).with_empty_chunk(1),
        );
        assert_eq!(
            error(source, PmcRequest::new("metadata/a.json"))
                .await
                .kind(),
            ErrorKind::Io
        );
    }
}

struct PendingSource {
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

struct PendingReader {
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    metadata: ByteMetadata,
}

#[async_trait]
impl Accessor for PendingSource {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            ..Capability::default()
        }
    }
    async fn open(&self, _: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        Ok(Box::new(PendingReader {
            polls: self.polls.clone(),
            drops: self.drops.clone(),
            metadata: ByteMetadata::default(),
        }))
    }
}
#[async_trait]
impl ByteAccessor for PendingSource {}
#[async_trait]
impl Reader for PendingReader {
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
        self.polls.fetch_add(1, Ordering::SeqCst);
        std::future::pending().await
    }
}
#[async_trait]
impl ByteReader for PendingReader {}
impl Drop for PendingReader {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn cancelling_pending_consumption_releases_the_active_byte_reader() {
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let source = PmcAccessor::new(PendingSource {
        polls: polls.clone(),
        drops: drops.clone(),
    });
    let mut reader = source
        .open(PmcRequest::new("metadata/a.json"))
        .await
        .unwrap();
    let mut next = Box::pin(reader.next());
    assert!(futures::poll!(next.as_mut()).is_pending());
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(next);
    drop(reader);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(polls.load(Ordering::SeqCst), 1);
}

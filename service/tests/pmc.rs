use arrow_array::{Array, BooleanArray, LargeStringArray, ListArray, StringArray};
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteAccessor, ByteReadOptions, ErrorKind, ReadOptions, ReadRequest,
};
use open_arrow_access_decompression::{DecompressionLayer, Gzip};
use open_arrow_access_service::pmc::{PmcAccessor, PmcLimits, schema};
use open_arrow_access_test_support::MemoryAccessor;
use std::num::NonZeroUsize;

const RAW: &str = include_str!("../../testdata/pmc/metadata.json");

#[tokio::test]
async fn preserves_upstream_fields_and_raw_json_without_content_reads() {
    let source = MemoryAccessor::new(RAW.as_bytes(), 1);
    let observed = source.observations.clone();
    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        io_chunk_size: NonZeroUsize::new(11),
    };
    let accessor = PmcAccessor::new(source);
    let mut reader = accessor
        .open(ReadRequest::new("metadata/a.json").with_byte_options(options))
        .await
        .unwrap();
    assert!(observed.requests.lock().unwrap().is_empty());
    assert_eq!(reader.metadata().schema, schema());
    assert_eq!(
        reader.metadata().source.as_ref().unwrap().locator(),
        "metadata/a.json"
    );
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 16);
    assert_eq!(
        batch
            .column_by_name("pmcid")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        " pmc00123 "
    );
    assert_eq!(
        batch
            .column_by_name("license_code")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "CC BY"
    );
    assert_eq!(
        batch
            .column_by_name("pmid")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "36537061"
    );
    assert!(
        batch
            .column_by_name("is_pmc_openaccess")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert_eq!(
        batch
            .column_by_name("raw_metadata_json")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .value(0),
        RAW
    );
    assert!(batch.schema().field_with_name("pdf_bytes").is_err());
    assert!(reader.next().await.unwrap().is_none());
    let requests = observed.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].object.locator(), "metadata/a.json");
    assert_eq!(requests[0].options, options);
}

#[tokio::test]
async fn missing_pdf_noncommercial_manuscripts_and_unknown_fields_still_produce_rows() {
    for pdf in [
        "",
        ",\"pdf_url\":null",
        ",\"pdf_url\":\"https://mirror.example/paper.pdf?token=value\"",
    ] {
        let raw = format!(
            r#"{{"pmcid":"PMC1","version":null,"license_code":"TDM","is_pmc_openaccess":false,"is_manuscript":true,"media_urls":["s3://bucket/image.png"],"future_field":1e9999{pdf}}}"#
        );
        let mut reader = PmcAccessor::new(MemoryAccessor::new(raw.as_bytes(), 1))
            .open(ReadRequest::new("a.json"))
            .await
            .unwrap();
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(batch.column_by_name("version").unwrap().is_null(0));
        assert_eq!(
            batch.column_by_name("pdf_url").unwrap().is_null(0),
            !pdf.contains("https")
        );
        assert_eq!(
            batch
                .column_by_name("media_urls")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value_length(0),
            1
        );
        assert_eq!(
            batch
                .column_by_name("raw_metadata_json")
                .unwrap()
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(0),
            raw
        );
    }
}

#[tokio::test]
async fn projections_keep_order_and_zero_column_row_counts() {
    let full_schema = schema();
    let all_names = full_schema
        .fields()
        .iter()
        .rev()
        .map(|field| field.name().as_str())
        .collect();
    let mut full_reader = PmcAccessor::new(MemoryAccessor::new(RAW.as_bytes(), 1))
        .open(ReadRequest::new("a.json"))
        .await
        .unwrap();
    let full_batch = full_reader.next().await.unwrap().unwrap();
    for names in [vec!["pdf_url", "pmcid"], all_names, vec![]] {
        let request = ReadRequest::new("a.json").with_options(ReadOptions {
            projection: Some(names.iter().map(|s| (*s).into()).collect()),
            ..ReadOptions::default()
        });
        let mut reader = PmcAccessor::new(MemoryAccessor::new(RAW.as_bytes(), 1))
            .open(request)
            .await
            .unwrap();
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), names.len());
        assert!(Arc::ptr_eq(&batch.schema(), &reader.metadata().schema));
        let indices = names
            .iter()
            .map(|name| full_schema.index_of(name).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(batch, full_batch.project(&indices).unwrap());
    }
    let input = MemoryAccessor::new(RAW.as_bytes(), 1);
    let observed = input.observations.clone();
    let accessor = PmcAccessor::new(input);
    for names in [vec!["pdf_bytes"], vec!["pmcid", "pmcid"]] {
        let request = ReadRequest::new("a.json").with_options(ReadOptions {
            projection: Some(names.iter().map(|s| (*s).into()).collect()),
            ..ReadOptions::default()
        });
        assert_eq!(
            accessor.open(request).await.err().unwrap().kind(),
            ErrorKind::InvalidInput
        );
    }
    assert!(observed.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn locators_are_forwarded_unchanged_to_the_byte_accessor() {
    for locator in [
        "https://example.com/article.json?version=1#metadata",
        "s3://pmc-oa-opendata/metadata/PMC1.1.json",
        "/absolute/path/article.json",
    ] {
        let source = MemoryAccessor::new(br#"{"pmcid":"PMC1"}"#, 64);
        let observed = source.observations.clone();
        let mut reader = PmcAccessor::new(source)
            .open(ReadRequest::new(locator))
            .await
            .unwrap();
        assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
        assert_eq!(
            reader.metadata().source.as_ref().unwrap().locator(),
            locator
        );
        let requests = observed.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].object.locator(), locator);
    }
}

#[tokio::test]
async fn media_lists_preserve_null_empty_and_populated_values() {
    for (value, expected) in [("null", None), ("[]", Some(0)), (r#"["a", "b"]"#, Some(2))] {
        let raw = format!(r#"{{"pmcid":"PMC1","media_urls":{value}}}"#);
        let mut reader = PmcAccessor::new(MemoryAccessor::new(raw.as_bytes(), 64))
            .open(ReadRequest::new("a.json"))
            .await
            .unwrap();
        let batch = reader.next().await.unwrap().unwrap();
        let urls = batch
            .column_by_name("media_urls")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(urls.is_null(0), expected.is_none());
        if let Some(length) = expected {
            assert_eq!(urls.value_length(0), length);
        }
    }
}

#[tokio::test]
async fn projection_does_not_skip_validation_of_omitted_fields() {
    for (field, value) in [
        ("pmcid", "null"),
        ("version", "0"),
        ("pmid", "1.5"),
        ("doi", "42"),
        ("title", "false"),
        ("is_manuscript", r#""true""#),
        ("media_urls", "[null]"),
    ] {
        // The final duplicate wins, including invalid values.
        let raw = format!(r#"{{"pmcid":"PMC1","{field}":{value}}}"#);
        for projection in [vec!["pmcid".into()], vec![]] {
            let mut reader = PmcAccessor::new(MemoryAccessor::new(raw.as_bytes(), 64))
                .open(ReadRequest::new("a.json").with_options(ReadOptions {
                    projection: Some(projection),
                    ..ReadOptions::default()
                }))
                .await
                .unwrap();
            assert_eq!(
                reader.next().await.unwrap_err().kind(),
                ErrorKind::Decode,
                "{field}={value}"
            );
            assert!(reader.next().await.unwrap().is_none());
        }
    }
}

#[tokio::test]
async fn malformed_metadata_and_invalid_values_are_terminal() {
    for raw in [
        "",
        "[]",
        "null",
        "42",
        "{",
        "{} {}",
        "{\"pmcid\":\"PMC1\"} trailing",
        "{}",
        r#"{"pmcid":"PMC0"}"#,
        r#"{"pmcid":"PMC1","version":0}"#,
        r#"{"pmcid":"PMC1","version":4294967296}"#,
        r#"{"pmcid":"PMC1","pdf_url":false}"#,
        r#"{"pmcid":"PMC1","media_urls":false}"#,
        r#"{"pmcid":"PMC1","is_manuscript":"true"}"#,
    ] {
        let mut reader = PmcAccessor::new(MemoryAccessor::new(raw.as_bytes(), 1))
            .open(ReadRequest::new("a.json"))
            .await
            .unwrap();
        let failure = reader.next().await.unwrap_err();
        assert_eq!(failure.kind(), ErrorKind::Decode, "{raw}");
        assert!(failure.to_string().contains("a.json"));
        assert!(reader.next().await.unwrap().is_none());
    }
    let mut reader = PmcAccessor::new(MemoryAccessor::new(&[0xff], 1))
        .open(ReadRequest::new("a.json"))
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
}

#[tokio::test]
async fn metadata_limits_apply_before_and_during_decoded_reads() {
    let limits = PmcLimits {
        metadata_bytes: NonZeroUsize::new(RAW.len() - 1).unwrap(),
    };
    let mut reader = PmcAccessor::new(MemoryAccessor::new(RAW.as_bytes(), 1))
        .with_limits(limits)
        .open(ReadRequest::new("a.json"))
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
    let compressed = MemoryAccessor::new(include_bytes!("../../testdata/pmc/metadata.json.gz"), 1);
    let decoded = compressed.layer(DecompressionLayer::new(Gzip));
    let accessor = PmcAccessor::new(decoded);
    let mut reader = accessor.open(ReadRequest::new("a.json")).await.unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
    let mut reader = accessor
        .with_limits(limits)
        .open(ReadRequest::new("a.json"))
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn source_errors_and_empty_chunks_remain_terminal() {
    for input in [
        MemoryAccessor::new(RAW.as_bytes(), 1).with_failure(5, ErrorKind::PermissionDenied),
        MemoryAccessor::new(RAW.as_bytes(), 1).with_empty_chunk(1),
    ] {
        let mut reader = PmcAccessor::new(input)
            .open(ReadRequest::new("a.json"))
            .await
            .unwrap();
        assert!(reader.next().await.is_err());
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn duplicate_keys_empty_strings_and_independent_opens_are_preserved() {
    let raw = br#"{"pmcid":"PMC1","pmid":1,"pmid":"00001","doi":"","pdf_url":null}"#;
    let accessor = PmcAccessor::new(MemoryAccessor::new(raw, 1));
    let mut a = accessor.open(ReadRequest::new("a.json")).await.unwrap();
    let mut b = accessor.open(ReadRequest::new("a.json")).await.unwrap();
    let batch = a.next().await.unwrap().unwrap();
    assert_eq!(batch, b.next().await.unwrap().unwrap());
    assert_eq!(
        batch
            .column_by_name("pmid")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "00001"
    );
    assert_eq!(
        batch
            .column_by_name("doi")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        ""
    );
}

use open_arrow_access_core::{
    ByteMetadata, ByteReadRequest, ByteReader, Bytes, Capability, Reader, Result, async_trait,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
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
        .clone()
        .open(ReadRequest::new("metadata/a.json"))
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

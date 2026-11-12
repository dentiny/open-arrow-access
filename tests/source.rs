use open_arrow_access::{
    Bytes, ErrorKind, ObjectRef, OrderRequirement, RecordBatch, RecordMetadata, RecordReader,
    Result, Schema, StreamReader, async_trait, bytes_reader,
    source::{FormatHandler, SourceHandler, SourceInput, SourceRegistry, SourceRequest},
};
#[cfg(any(feature = "format-json", feature = "format-csv"))]
use std::io::Write;
use std::sync::Arc;

fn native_reader() -> Box<dyn RecordReader> {
    let schema = Arc::new(Schema::empty());
    Box::new(StreamReader::records(
        futures::stream::iter([Ok(RecordBatch::new_empty(schema.clone()))]),
        RecordMetadata {
            schema,
            source: None,
        },
        OrderRequirement::PreserveGlobal,
    ))
}
struct Native;
#[async_trait]
impl SourceHandler for Native {
    async fn open(&self, _: &SourceRequest) -> Result<SourceInput> {
        Ok(SourceInput::Records(native_reader()))
    }
}
struct OriginalBytes;
#[async_trait]
impl SourceHandler for OriginalBytes {
    async fn open(&self, _: &SourceRequest) -> Result<SourceInput> {
        Ok(SourceInput::Bytes(Box::new(bytes_reader(
            Bytes::from_static(b"original bytes"),
            Some(ObjectRef::new("file.custom")),
        ))))
    }
}
struct CustomFormat;
#[async_trait]
impl FormatHandler for CustomFormat {
    async fn decode(
        &self,
        mut input: Box<dyn open_arrow_access::ByteReader>,
        _: &SourceRequest,
    ) -> Result<Box<dyn RecordReader>> {
        assert_eq!(
            input.next().await?.unwrap(),
            Bytes::from_static(b"original bytes")
        );
        assert!(input.next().await?.is_none());
        Ok(native_reader())
    }
}
#[tokio::test]
async fn native_service_and_custom_format_share_one_open_entry_point() {
    let mut registry = SourceRegistry::empty();
    registry.register_source("native", Arc::new(Native));
    registry.register_source("bytes", Arc::new(OriginalBytes));
    registry.register_format("custom", &["custom"], Arc::new(CustomFormat));
    let mut native = registry
        .open(SourceRequest::new("native://identifier"))
        .await
        .unwrap();
    assert!(native.next().await.unwrap().is_some());
    assert!(native.next().await.unwrap().is_none());
    let mut decoded = registry
        .open(SourceRequest::new("bytes://identifier"))
        .await
        .unwrap();
    assert!(decoded.next().await.unwrap().is_some());
    let mut request = SourceRequest::new("opaque-provider-id");
    request.source = Some("native".into());
    assert!(registry.open(request).await.is_ok());
}
#[tokio::test]
async fn unsupported_selections_are_explicit_and_native_records_are_not_redecoded() {
    let mut registry = SourceRegistry::empty();
    registry.register_source("native", Arc::new(Native));
    let mut request = SourceRequest::new("native://id");
    request.format = Some("json".into());
    assert_eq!(
        registry.open(request).await.err().unwrap().kind(),
        ErrorKind::InvalidInput
    );
    let mut request = SourceRequest::new("id");
    request.source = Some("unregistered-provider".into());
    assert_eq!(
        registry.open(request).await.err().unwrap().kind(),
        ErrorKind::Unsupported
    );
    assert_eq!(
        registry
            .open(SourceRequest::new(""))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::InvalidInput
    );
}
#[cfg(feature = "format-json")]
#[tokio::test]
async fn json_inference_explicit_format_projection_and_batch_size() {
    let mut file = tempfile::Builder::new()
        .suffix(".jsonl")
        .tempfile()
        .unwrap();
    write!(
        file,
        "{{\"id\":1,\"text\":\"one\"}}\n{{\"id\":2,\"text\":null}}\n"
    )
    .unwrap();
    let mut request = SourceRequest::new(file.path().to_str().unwrap());
    request.options.batch_size = std::num::NonZeroUsize::new(1).unwrap();
    request.options.projection = Some(vec!["text".into()]);
    let mut reader = SourceRegistry::default().open(request).await.unwrap();
    assert_eq!(reader.metadata().schema.fields().len(), 1);
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
    assert_eq!(
        reader.next().await.unwrap().unwrap().column(0).null_count(),
        1
    );
    assert!(reader.next().await.unwrap().is_none());
    let mut no_suffix = tempfile::NamedTempFile::new().unwrap();
    writeln!(no_suffix, "{{\"id\":1}}").unwrap();
    let mut request = SourceRequest::new(no_suffix.path().to_str().unwrap());
    request.format = Some("json".into());
    assert!(SourceRegistry::default().open(request).await.is_ok());
}
#[cfg(feature = "format-csv")]
#[tokio::test]
async fn csv_tsv_and_file_uri() {
    for (suffix, body) in [
        (".csv", "id,name\n1,Alice\n2,Bob\n"),
        (".tsv", "id\tname\n1\tAlice\n2\tBob\n"),
    ] {
        let mut file = tempfile::Builder::new().suffix(suffix).tempfile().unwrap();
        file.write_all(body.as_bytes()).unwrap();
        let mut request =
            SourceRequest::new(url::Url::from_file_path(file.path()).unwrap().to_string());
        request
            .format_options
            .insert("header".into(), "true".into());
        let mut reader = SourceRegistry::default().open(request).await.unwrap();
        assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 2);
    }
}
#[cfg(feature = "format-parquet")]
#[tokio::test]
async fn parquet_uses_ranged_source_and_preserves_schema() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("testdata/parquet/ordered-snappy.parquet");
    let mut reader = SourceRegistry::default()
        .open(SourceRequest::new(path.to_str().unwrap()))
        .await
        .unwrap();
    assert!(!reader.metadata().schema.fields().is_empty());
    assert!(reader.next().await.unwrap().unwrap().num_rows() > 0);
}
#[cfg(feature = "service-huggingface")]
#[tokio::test]
async fn hf_rejects_ambiguous_selection_before_network_access() {
    for locator in [
        "hf://models/owner/name",
        "hf://datasets/owner/name/extra",
        "hf://datasets/owner/name?split=train",
    ] {
        let error = SourceRegistry::default()
            .open(SourceRequest::new(locator))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
    assert_eq!(
        SourceRegistry::default()
            .open(SourceRequest::new("hf://datasets/owner/name"))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::InvalidInput
    );
}

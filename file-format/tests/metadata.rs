#![cfg(any(
    feature = "json",
    feature = "csv",
    feature = "parquet",
    all(feature = "atif", feature = "warc"),
))]

use open_arrow_access_core::{Accessor, ReadRequest};
use open_arrow_access_test_support::{MemoryAccessor, assert_metadata_matches_open};

#[cfg(feature = "json")]
#[tokio::test]
async fn json_inference_is_bounded_and_metadata_matches_projection() {
    use open_arrow_access_file_format::json::{JsonAccessor, JsonInferenceOptions};
    let input = MemoryAccessor::new(b"{\"id\":1,\"v\":NaN}\nnot JSON after the sample", 1);
    let observed = input.observations.clone();
    let source = JsonAccessor::infer(input).with_inference_options(JsonInferenceOptions {
        max_records: 1,
        max_bytes: 1024,
    });
    let mut request = ReadRequest::new("object");
    request.options.projection = Some(vec!["v".into(), "id".into()]);
    let metadata = source.metadata(request.clone()).await.unwrap();
    assert_eq!(metadata.schema.field(0).name(), "v");
    assert!(observed.reads.load(std::sync::atomic::Ordering::SeqCst) < 25);
    assert_metadata_matches_open(&source, request).await;
}

#[cfg(feature = "csv")]
#[tokio::test]
async fn csv_inference_obeys_dialect_and_budget() {
    use open_arrow_access_file_format::csv::{CsvAccessor, CsvHeader, CsvInferenceOptions};
    let input = MemoryAccessor::new(b"id\tname\n1\tx\nnot\tan\textra\trow\n", 1);
    let source = CsvAccessor::infer(input)
        .with_header(CsvHeader::Present)
        .with_inference_options(CsvInferenceOptions {
            max_records: 2,
            ..Default::default()
        });
    let mut request = ReadRequest::new("object.tsv");
    request.options.projection = Some(vec!["name".into(), "id".into()]);
    assert_metadata_matches_open(&source, request).await;
}

#[cfg(all(feature = "atif", feature = "warc"))]
#[tokio::test]
async fn fixed_schemas_do_not_open_storage() {
    use open_arrow_access_core::ErrorKind;
    use open_arrow_access_file_format::{atif::AtifAccessor, warc::WarcArrowAccessor};
    let input = MemoryAccessor::new(b"", 1).with_open_failure(ErrorKind::Io);
    let request = ReadRequest::new("missing");
    assert!(
        AtifAccessor::new(input.clone(), Default::default())
            .metadata(request.clone())
            .await
            .is_ok()
    );
    assert!(
        WarcArrowAccessor::new(input)
            .metadata(request)
            .await
            .is_ok()
    );
    assert_metadata_matches_open(
        &WarcArrowAccessor::new(MemoryAccessor::new(b"", 1)),
        ReadRequest::new("object"),
    )
    .await;
}

#[cfg(feature = "parquet")]
#[tokio::test]
async fn parquet_reads_only_footer_for_metadata() {
    use open_arrow_access_file_format::parquet::ParquetAccessor;
    let bytes = std::fs::read(open_arrow_access_test_support::fixture_path!(
        "parquet",
        "ordered-uncompressed.parquet"
    ))
    .unwrap();
    let footer_len =
        u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap()) as usize;
    let footer_start = bytes.len() - 8 - footer_len;
    let input = MemoryAccessor::new(&bytes, 1).with_ranges();
    let observed = input.observations.clone();
    let source = ParquetAccessor::new(input);
    let mut request = ReadRequest::new("object.parquet");
    request.options.projection = Some(vec!["title".into(), "id".into()]);
    source.metadata(request.clone()).await.unwrap();
    assert_eq!(observed.reads.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(
        observed
            .ranges
            .lock()
            .unwrap()
            .iter()
            .all(|range| range.start >= footer_start as u64)
    );
    assert_metadata_matches_open(&source, request).await;
}

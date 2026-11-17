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

#[cfg(feature = "json")]
#[tokio::test]
async fn json_inspection_distinguishes_empty_input_from_zero_column_rows() {
    use open_arrow_access_file_format::json::JsonAccessor;
    for (bytes, absent) in [(b"".as_slice(), true), (b" \n", true), (b"{}", false)] {
        let source = std::sync::Arc::new(JsonAccessor::infer(MemoryAccessor::new(bytes, 1)));
        let request = ReadRequest::new("object.json");
        let inspected = source.inspect_metadata(request.clone()).await.unwrap();
        assert_eq!(inspected.is_none(), absent);
        let metadata = source.metadata(request.clone()).await.unwrap();
        assert!(metadata.schema.fields().is_empty());
        if let Some(inspected) = inspected {
            assert_eq!(inspected.schema, metadata.schema);
            assert_eq!(inspected.source, metadata.source);
        }
        assert_metadata_matches_open(&source, request).await;
    }
}

#[cfg(feature = "json")]
#[tokio::test]
async fn json_inspection_preserves_explicit_schema_without_opening_storage() {
    use arrow_schema::{DataType, Field, Schema};
    use open_arrow_access_core::ErrorKind;
    use open_arrow_access_file_format::json::JsonAccessor;
    use std::sync::Arc;
    let input = MemoryAccessor::new(b"", 1).with_open_failure(ErrorKind::Io);
    let observed = input.observations.clone();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let source = JsonAccessor::new(input, schema.clone());
    let metadata = source
        .inspect_metadata(ReadRequest::new("object.json"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(metadata.schema, schema);
    assert!(observed.requests.lock().unwrap().is_empty());
}

#[cfg(feature = "csv")]
#[tokio::test]
async fn csv_inspection_distinguishes_empty_input_and_inferred_schema() {
    use open_arrow_access_file_format::csv::{CsvAccessor, CsvHeader};
    for (bytes, absent) in [(b"".as_slice(), true), (b"id\n1\n", false)] {
        let source =
            CsvAccessor::infer(MemoryAccessor::new(bytes, 1)).with_header(CsvHeader::Present);
        let request = ReadRequest::new("object.csv");
        let inspected = source.inspect_metadata(request.clone()).await.unwrap();
        assert_eq!(inspected.is_none(), absent);
        if let Some(metadata) = inspected {
            assert_eq!(metadata.schema.field(0).name(), "id");
        }
        assert_metadata_matches_open(&source, request).await;
    }
}

#[cfg(feature = "csv")]
#[tokio::test]
async fn csv_header_only_inspection_requires_column_types() {
    use arrow_schema::{DataType, Field, Schema};
    use open_arrow_access_file_format::csv::{CsvAccessor, CsvHeader};
    use std::sync::Arc;
    let input = MemoryAccessor::new(b"id\n", 1);
    let request = ReadRequest::new("object.csv");
    assert!(
        CsvAccessor::infer(input.clone())
            .with_header(CsvHeader::Present)
            .inspect_metadata(request.clone())
            .await
            .is_err()
    );
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let source = CsvAccessor::new(input, schema.clone()).with_header(CsvHeader::Present);
    assert_eq!(
        source
            .inspect_metadata(request.clone())
            .await
            .unwrap()
            .unwrap()
            .schema,
        schema
    );
    assert_metadata_matches_open(&source, request).await;
}

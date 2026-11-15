#![cfg(feature = "format-json")]

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use open_arrow_access::{
    Accessor, ErrorKind, OuterCompression, PreparedSource, ReadOptions, ReadRequest, file_accessor,
    file_format::json::JsonFormat, open_file_source,
};
use open_arrow_access_test_support::fs_operator;

fn operator() -> opendal::Operator {
    fs_operator(concat!(env!("CARGO_MANIFEST_DIR"), "/testdata"))
}

#[tokio::test]
async fn shared_accessor_supports_bound_files_and_prepared_projections() {
    let accessor = file_accessor(operator(), JsonFormat::infer(), OuterCompression::None);
    let prepared = PreparedSource::prepare(accessor, ReadRequest::new("json/projection.jsonl"))
        .await
        .unwrap();
    let bound = open_file_source(
        operator(),
        ReadRequest::new("json/projection.jsonl"),
        JsonFormat::infer(),
        OuterCompression::None,
    )
    .await
    .unwrap();
    assert_eq!(prepared.schema(), bound.schema());
    let mut reader = prepared
        .scan(ReadOptions {
            projection: Some(vec!["title".into()]),
            ..Default::default()
        })
        .await
        .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.schema().field(0).name(), "title");
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.num_rows(), 2);
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn invalid_projection_is_rejected_before_opening_missing_storage() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let accessor = file_accessor(operator(), JsonFormat::new(schema), OuterCompression::Auto);
    let request = ReadRequest::new("missing.json").with_options(ReadOptions {
        projection: Some(vec!["unknown".into()]),
        ..Default::default()
    });
    let error = accessor.open(request).await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
}

#[tokio::test]
#[cfg(feature = "decompression-gzip")]
async fn compression_choice_keeps_raw_bytes_or_decodes_the_outer_stream() {
    use open_arrow_access::{ByteReadRequest, read_all};

    let raw = file_accessor(operator(), JsonFormat::infer(), OuterCompression::None);
    let decoded = file_accessor(operator(), JsonFormat::infer(), OuterCompression::Auto);
    let request = ByteReadRequest::new("gzip/payload.gz");
    let raw = raw.storage().open(request.clone()).await.unwrap();
    assert!(raw.capability().read_with_range);
    let decoded = decoded.storage().open(request).await.unwrap();
    let raw_bytes = read_all(raw, "test").await.unwrap();
    let decoded_bytes = read_all(decoded, "test").await.unwrap();
    assert_eq!(&raw_bytes[..2], &[0x1f, 0x8b]);
    assert_eq!(decoded_bytes, b"payload");
}

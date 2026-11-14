use open_arrow_access::{
    FileSource, OuterCompression, ReadOptions, ReadRequest, ReaderFactory, RecordReader,
    file_format::{
        csv::{CsvFileFormat, CsvInferenceOptions},
        json::JsonFormat,
        parquet::ParquetFormat,
        xml::XmlFormat,
    },
    open_file_source,
};
use open_arrow_access_test_support::{fixture_path, fs_operator};

async fn assert_rows(source: &FileSource, expected: usize) {
    let mut reader = source.reader().await.unwrap();
    assert_eq!(reader.metadata().schema, source.schema());
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.schema(), source.schema());
        assert!(batch.num_rows() <= 2);
        rows += batch.num_rows();
    }
    assert_eq!(rows, expected);
}

async fn check_format<F>(format: F, kind: &str, name: &str, projection: Vec<String>, rows: usize)
where
    F: ReaderFactory<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
{
    let path = fixture_path!(kind, name);
    let source = open_file_source(
        fs_operator(path.parent().unwrap()),
        ReadRequest::new(name).with_options(ReadOptions {
            batch_size: 2.try_into().unwrap(),
            projection: Some(projection.clone()),
        }),
        format,
        OuterCompression::None,
    )
    .await
    .unwrap();
    assert_eq!(source.schema().fields().len(), projection.len());
    assert_rows(&source, rows).await;
    assert_rows(&source, rows).await;
}

#[tokio::test]
async fn typed_formats_preserve_options_and_can_be_reopened() {
    check_format(
        CsvFileFormat::infer(),
        "csv",
        "good.csv",
        vec!["id".into()],
        5,
    )
    .await;
    check_format(
        JsonFormat::infer(),
        "json",
        "good.json",
        vec!["id".into()],
        4,
    )
    .await;
    check_format(
        XmlFormat::new("/inventory/warehouse"),
        "xml",
        "inventory.xml",
        vec![],
        2,
    )
    .await;
    check_format(
        ParquetFormat,
        "parquet",
        "ordered-snappy.parquet",
        vec!["id".into()],
        7,
    )
    .await;
}

#[tokio::test]
async fn compression_policy_is_explicit_and_raw_parquet_keeps_ranges() {
    let path = fixture_path!("pmc/inventory", "first.csv.gz");
    let source = open_file_source(
        fs_operator(path.parent().unwrap()),
        ReadRequest::new("first.csv.gz").with_options(ReadOptions {
            batch_size: 2.try_into().unwrap(),
            ..ReadOptions::default()
        }),
        CsvFileFormat::infer().with_inference_options(CsvInferenceOptions {
            has_header: Some(false),
            ..CsvInferenceOptions::default()
        }),
        OuterCompression::Auto,
    )
    .await
    .unwrap();
    assert_rows(&source, 2).await;
    assert_rows(&source, 2).await;

    let dir = tempfile::tempdir().unwrap();
    std::fs::copy(
        fixture_path!("parquet", "ids.parquet"),
        dir.path().join("raw.parquet.gz"),
    )
    .unwrap();
    let source = open_file_source(
        fs_operator(dir.path()),
        ReadRequest::new("raw.parquet.gz").with_options(ReadOptions {
            batch_size: 2.try_into().unwrap(),
            ..ReadOptions::default()
        }),
        ParquetFormat,
        OuterCompression::None,
    )
    .await
    .unwrap();
    assert_rows(&source, 3).await;
}

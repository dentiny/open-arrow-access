use arrow_array::cast::AsArray;
use arrow_schema::{DataType, Field, Schema};
use open_arrow_access_core::{Bytes, ErrorKind, ReadRequest};
use open_arrow_access_file_format::{
    csv::CsvFileFormat,
    json::JsonFormat,
    read_document,
    xml::{XmlField, XmlFormat, XmlSchema},
};
use std::sync::Arc;

#[tokio::test]
async fn documents_use_each_formats_schema_and_parser() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let json = read_document(
        Bytes::from_static(br#"{"value":7}"#),
        JsonFormat::new(schema.clone()),
        ReadRequest::new("row.json"),
    )
    .await
    .unwrap();
    let csv = read_document(
        Bytes::from_static(b"7\n"),
        CsvFileFormat::new(schema),
        ReadRequest::new("row.csv"),
    )
    .await
    .unwrap();
    let xml = XmlFormat::new("/row").with_schema(
        XmlSchema::new(vec![XmlField::new(
            "value",
            "value",
            DataType::Int64,
            false,
        )])
        .unwrap(),
    );
    let xml = read_document(
        Bytes::from_static(b"<row><value>7</value></row>"),
        xml,
        ReadRequest::new("row.xml"),
    )
    .await
    .unwrap();
    for batch in [json, csv, xml] {
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(0),
            7
        );
    }
}

#[tokio::test]
async fn documents_require_one_row_and_validate_the_complete_input() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    for raw in [
        "",
        "{}",
        r#"{"value":1} {"value":2}"#,
        r#"{"value":1} trailing"#,
    ] {
        let error = read_document(
            Bytes::copy_from_slice(raw.as_bytes()),
            JsonFormat::new(schema.clone()),
            ReadRequest::new("row.json"),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Decode, "{raw}");
    }
}

#[tokio::test]
async fn opened_input_validates_before_reading_and_preserves_source_failures() {
    use open_arrow_access_core::{Accessor, ByteReadRequest, ReadOptions, ReaderFactoryExt};
    use open_arrow_access_test_support::MemoryAccessor;
    use std::sync::atomic::Ordering;

    let source = MemoryAccessor::new(b"{}", 1).with_failure(0, ErrorKind::PermissionDenied);
    let observed = source.observations.clone();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let format = JsonFormat::new(schema);
    let request = ReadRequest::new("row.json").with_options(ReadOptions {
        projection: Some(vec!["missing".into()]),
        ..Default::default()
    });
    let input = source.open(ByteReadRequest::new("row.json")).await.unwrap();
    let error = format.open_input(input, request).await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(observed.reads.load(Ordering::Relaxed), 0);

    let input = source.open(ByteReadRequest::new("row.json")).await.unwrap();
    let mut reader = format
        .open_input(input, ReadRequest::new("row.json"))
        .await
        .unwrap();
    assert_eq!(
        reader.next().await.unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
    assert!(reader.next().await.unwrap().is_none());
}

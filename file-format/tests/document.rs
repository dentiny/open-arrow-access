use arrow_array::{cast::AsArray, types::Int64Type};
use arrow_schema::{DataType, Field, Schema};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, Bytes, ErrorKind, ReadOptions, ReadRequest, ReaderFactory,
    ReaderFactoryExt, RecordBatch, RecordReader, Result, SchemaRef,
};
use open_arrow_access_file_format::{
    csv::CsvFileFormat,
    json::JsonFormat,
    read_document,
    xml::{XmlField, XmlFormat, XmlSchema},
};
use open_arrow_access_test_support::MemoryAccessor;
use std::sync::{Arc, atomic::Ordering};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

async fn read(
    raw: &'static [u8],
    format: impl ReaderFactory<Request = ReadRequest, Reader = dyn RecordReader>,
) -> Result<RecordBatch> {
    read_document(
        Bytes::from_static(raw),
        format,
        ReadRequest::new("document"),
    )
    .await
}

#[tokio::test]
async fn reads_each_format() {
    let xml = XmlFormat::new("/row").with_schema(
        XmlSchema::new(vec![XmlField::new(
            "value",
            "value",
            DataType::Int64,
            false,
        )])
        .unwrap(),
    );
    for batch in [
        read(br#"{"value":7}"#, JsonFormat::new(schema()))
            .await
            .unwrap(),
        read(b"7\n", CsvFileFormat::new(schema())).await.unwrap(),
        read(b"<row><value>7</value></row>", xml).await.unwrap(),
    ] {
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), 7);
    }
}

#[tokio::test]
async fn rejects_invalid_documents() {
    for raw in [
        "",
        "{}",
        r#"{"value":1} {"value":2}"#,
        r#"{"value":1} trailing"#,
    ] {
        let error = read(raw.as_bytes(), JsonFormat::new(schema()))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Decode, "{raw}");
    }
}

#[tokio::test]
async fn validates_before_reading_and_preserves_source_errors() {
    let source = MemoryAccessor::new(b"{}", 1).with_failure(0, ErrorKind::PermissionDenied);
    let format = JsonFormat::new(schema());
    let request = ReadRequest::new("document").with_options(ReadOptions {
        projection: Some(vec!["missing".into()]),
        ..Default::default()
    });
    let input = source.open(ByteReadRequest::new("document")).await.unwrap();
    let error = format.open_input(input, request).await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(source.observations.reads.load(Ordering::Relaxed), 0);

    let input = source.open(ByteReadRequest::new("document")).await.unwrap();
    let mut reader = format
        .open_input(input, ReadRequest::new("document"))
        .await
        .unwrap();
    assert_eq!(
        reader.next().await.unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
    assert!(reader.next().await.unwrap().is_none());
}

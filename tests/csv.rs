#![cfg(feature = "format-csv")]

use std::{
    num::NonZeroUsize,
    sync::{Arc, atomic::Ordering},
};

use arrow_array::{Array, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use open_arrow_access::{
    Accessor, ByteReadOptions, ErrorKind, ReadOptions, ReadRequest, RecordBatch, SchemaRef,
    file_format::{CsvAccessor, CsvFormat},
};

mod common;

use common::MemoryAccessor;

fn schema() -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("title", DataType::Utf8, true),
        ],
        [("source".into(), "csv-fixture".into())].into(),
    ))
}

fn options(batch_size: usize, projection: Option<Vec<&str>>) -> ReadOptions {
    ReadOptions {
        batch_size: NonZeroUsize::new(batch_size).unwrap(),
        projection: projection.map(|names| names.into_iter().map(String::from).collect()),
    }
}

async fn collect(
    accessor: &impl Accessor<Request = ReadRequest, Reader = dyn open_arrow_access::RecordReader>,
    request: ReadRequest,
) -> Vec<RecordBatch> {
    let mut reader = accessor.open(request).await.unwrap();
    let mut batches = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        assert!(batch.num_rows() > 0);
        assert_eq!(batch.schema(), reader.metadata().schema);
        batches.push(batch);
    }
    assert!(reader.next().await.unwrap().is_none());
    batches
}

fn ids(batches: &[RecordBatch], column: usize) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

#[tokio::test]
async fn fragmented_csv_preserves_values_nulls_order_schema_and_source() {
    let input = b"id,title\r\n1,\"hello,world\"\r\n2,\r\n3,\"two\nlines\"\r\n4,\"say \"\"yes\"\"\"\r\n5,last";
    let bytes = MemoryAccessor::new(input, 1);
    let accessor = CsvAccessor::new(bytes, schema()).with_format(
        CsvFormat::default()
            .with_header(true)
            .with_header_validation(true),
    );
    assert!(accessor.capability().read);
    assert!(!accessor.capability().read_with_range);
    let request = ReadRequest::new("data.csv").with_options(options(2, None));
    let mut reader = accessor.open(request).await.unwrap();
    assert_eq!(reader.metadata().schema, schema());
    assert_eq!(
        reader.metadata().source.as_ref().unwrap().locator(),
        "data.csv"
    );
    assert!(!reader.capability().read_with_range);
    let mut batches = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        batches.push(batch);
    }
    assert_eq!(
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        [2, 2, 1]
    );
    assert_eq!(ids(&batches, 0), [1, 2, 3, 4, 5]);
    let title = |batch: usize| {
        batches[batch]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
    };
    assert_eq!(title(0).value(0), "hello,world");
    assert!(title(0).is_null(1));
    assert_eq!(title(1).value(0), "two\nlines");
    assert_eq!(title(1).value(1), "say \"yes\"");
    assert_eq!(batches[0].schema().metadata(), schema().metadata());
}

#[tokio::test]
async fn tsv_handles_utf8_and_configured_header_without_a_final_newline() {
    let accessor = CsvAccessor::new(
        MemoryAccessor::new("id\ttitle\n1\t你好\n2\t\"two\tcolumns\"".as_bytes(), 1),
        schema(),
    )
    .with_format(CsvFormat::default().with_delimiter(b'\t').with_header(true));
    let batches = collect(&accessor, ReadRequest::new("data.tsv")).await;
    assert_eq!(ids(&batches, 0), [1, 2]);
    let title = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(title.value(0), "你好");
    assert_eq!(title.value(1), "two\tcolumns");
}

#[tokio::test]
async fn projection_preserves_requested_order_metadata_and_zero_column_rows() {
    let accessor = CsvAccessor::new(MemoryAccessor::new(b"1,one\n2,two\n3,three", 4), schema());
    let batches = collect(
        &accessor,
        ReadRequest::new("data.csv").with_options(options(2, Some(vec!["title", "id"]))),
    )
    .await;
    assert_eq!(ids(&batches, 1), [1, 2, 3]);
    assert_eq!(batches[0].schema().field(0).name(), "title");
    assert_eq!(batches[0].schema().metadata(), schema().metadata());
    let empty = collect(
        &accessor,
        ReadRequest::new("data.csv").with_options(options(2, Some(vec![]))),
    )
    .await;
    assert_eq!(empty.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    assert!(empty.iter().all(|batch| batch.num_columns() == 0));
}

#[tokio::test]
async fn projection_and_input_schema_errors_are_rejected_before_opening() {
    let bytes = MemoryAccessor::new(b"1,one", 10);
    let accessor = CsvAccessor::new(bytes.clone(), schema());
    for columns in [vec!["missing"], vec!["id", "id"]] {
        let error = accessor
            .open(ReadRequest::new("data.csv").with_options(options(2, Some(columns))))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
    let empty = CsvAccessor::new(bytes.clone(), Arc::new(Schema::empty()));
    assert_eq!(
        empty
            .open(ReadRequest::new("data.csv"))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert!(bytes.observations.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn empty_and_header_only_inputs_emit_no_batches() {
    for (input, header) in [("", false), ("id,title", true), ("id,title\n", true)] {
        let accessor = CsvAccessor::new(MemoryAccessor::new(input.as_bytes(), 1), schema())
            .with_format(
                CsvFormat::default()
                    .with_header(header)
                    .with_header_validation(header),
            );
        assert!(
            collect(&accessor, ReadRequest::new("empty.csv"))
                .await
                .is_empty()
        );
    }
}

#[tokio::test]
async fn invalid_data_errors_are_terminal() {
    for input in [
        &b"1\n"[..],
        &b"1,one,extra\n"[..],
        &b"bad,one\n"[..],
        &b"1,\xff\n"[..],
    ] {
        let bytes = MemoryAccessor::new(input, 1);
        let accessor = CsvAccessor::new(bytes.clone(), schema());
        let mut reader = accessor.open(ReadRequest::new("bad.csv")).await.unwrap();
        let error = reader.next().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Decode);
        assert!(std::error::Error::source(&error).is_some());
        let calls = bytes.observations.reads.load(Ordering::SeqCst);
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(bytes.observations.reads.load(Ordering::SeqCst), calls);
    }
}

#[tokio::test]
async fn byte_options_are_forwarded_and_io_failures_preserve_kind_without_read_ahead() {
    let mut bytes = MemoryAccessor::new(b"1,one\n2,two\n", 6);
    bytes.fail_after = Some(6);
    let accessor = CsvAccessor::new(bytes.clone(), schema());
    let byte_options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        io_chunk_size: NonZeroUsize::new(7),
    };
    let mut reader = accessor
        .open(
            ReadRequest::new("data.csv")
                .with_options(options(1, None))
                .with_byte_options(byte_options),
        )
        .await
        .unwrap();
    assert_eq!(
        bytes.observations.requests.lock().unwrap()[0].options,
        byte_options
    );
    assert_eq!(bytes.observations.reads.load(Ordering::SeqCst), 0);
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(ids(&[batch], 0), [1]);
    assert_eq!(bytes.observations.reads.load(Ordering::SeqCst), 1);
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Io);
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(bytes.observations.reads.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn header_validation_and_truncated_row_policy_are_explicit() {
    let accessor = CsvAccessor::new(MemoryAccessor::new(b"wrong,title\n1,one", 1), schema())
        .with_format(
            CsvFormat::default()
                .with_header(true)
                .with_header_validation(true),
        );
    let mut reader = accessor.open(ReadRequest::new("header.csv")).await.unwrap();
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
    let accessor = CsvAccessor::new(MemoryAccessor::new(b"1\n2,two", 1), schema())
        .with_format(CsvFormat::default().with_truncated_rows(true));
    let batches = collect(&accessor, ReadRequest::new("short.csv")).await;
    assert_eq!(ids(&batches, 0), [1, 2]);
    assert!(batches[0].column(1).is_null(0));
}

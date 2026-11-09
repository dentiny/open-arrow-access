use std::{
    num::NonZeroUsize,
    sync::{Arc, atomic::Ordering},
};

use arrow_array::{Array, StringArray, types::Int64Type};
use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use open_arrow_access_core::{
    Accessor, ByteReadOptions, ErrorKind, ReadOptions, ReadRequest, RecordBatch, SchemaRef,
};

use open_arrow_access_file_format::csv::{CsvAccessor, CsvFormat};
use open_arrow_access_test_support::{
    MemoryAccessor, fixture,
    format::{self, collect_batches as collect},
    record::collect_values,
};

const BAD_FILES: [&str; 4] = [
    "bad_missing_column.csv",
    "bad_extra_column.csv",
    "bad_integer.csv",
    "bad_utf8.csv",
];

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

#[tokio::test]
async fn fragmented_csv_preserves_values_nulls_order_schema_and_source() {
    let input = fixture!("csv", "good.csv").await;
    let bytes = MemoryAccessor::new(&input, 1);
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
    assert_eq!(collect_values::<Int64Type>(&batches, 0), [1, 2, 3, 4, 5]);
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
    assert_eq!(title(2).value(0), "last");
    assert_eq!(batches[0].schema().metadata(), schema().metadata());
}

#[tokio::test]
async fn tsv_handles_utf8_and_configured_header_without_a_final_newline() {
    let accessor = CsvAccessor::new(
        MemoryAccessor::new(&fixture!("csv", "good.tsv").await, 1),
        schema(),
    )
    .with_format(CsvFormat::default().with_delimiter(b'\t').with_header(true));
    let batches = collect(&accessor, ReadRequest::new("data.tsv")).await;
    assert_eq!(collect_values::<Int64Type>(&batches, 0), [1, 2]);
    let title = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(title.value(0), "你好");
    assert_eq!(title.value(1), "two\tcolumns");
}

#[tokio::test]
async fn fixed_schema_contract_preserves_projection_and_source_failures() {
    let input = fixture!("csv", "good_no_header.csv").await;
    let batches = format::assert_fixed_schema_contract(
        |bytes| CsvAccessor::new(bytes, schema()),
        format::FormatCase {
            bytes: &input,
            request: ReadRequest::new("data.csv")
                .with_options(options(2, Some(vec!["title", "id"])))
                .with_byte_options(ByteReadOptions {
                    io_concurrency: NonZeroUsize::new(3).unwrap(),
                    io_chunk_size: NonZeroUsize::new(7),
                }),
            schema: schema(),
            rows: 3,
            fault_offsets: &[0, 6],
        },
    )
    .await;
    assert_eq!(collect_values::<Int64Type>(&batches, 1), [1, 2, 3]);
}

#[tokio::test]
async fn empty_input_schema_is_rejected_before_opening() {
    let bytes = MemoryAccessor::new(&fixture!("csv", "good_no_header.csv").await, 10);
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
async fn unsupported_csv_fields_are_configuration_errors_only_when_projected() {
    for data_type in [
        DataType::Binary,
        DataType::LargeUtf8,
        DataType::Duration(TimeUnit::Second),
        DataType::Interval(IntervalUnit::YearMonth),
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
    ] {
        let bytes = MemoryAccessor::new(&fixture!("csv", "good_no_header.csv").await, 1);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("title", data_type.clone(), true),
        ]));
        let accessor = CsvAccessor::new(bytes.clone(), schema);
        let error = accessor
            .open(ReadRequest::new("good_no_header.csv"))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::Unsupported, "{data_type:?}");
        assert_eq!(error.operation(), "open_csv");
        assert!(bytes.observations.requests.lock().unwrap().is_empty());
        let batches = collect(
            &accessor,
            ReadRequest::new("good_no_header.csv").with_options(options(2, Some(vec!["id"]))),
        )
        .await;
        assert_eq!(collect_values::<Int64Type>(&batches, 0), [1, 2, 3]);
        let batches = collect(
            &accessor,
            ReadRequest::new("good_no_header.csv").with_options(options(2, Some(vec![]))),
        )
        .await;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    }
}

#[tokio::test]
async fn empty_and_header_only_inputs_emit_no_batches() {
    for (name, header) in [
        ("empty.csv", false),
        ("header_only_no_newline.csv", true),
        ("header_only.csv", true),
    ] {
        let accessor = CsvAccessor::new(
            MemoryAccessor::new(&fixture!("csv", name).await, 1),
            schema(),
        )
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
    for chunk_size in [1, 4096] {
        for name in BAD_FILES {
            let bytes = MemoryAccessor::new(&fixture!("csv", name).await, chunk_size);
            let accessor = CsvAccessor::new(bytes.clone(), schema());
            let mut reader = accessor.open(ReadRequest::new(name)).await.unwrap();
            let error = format::assert_terminal_error(
                reader.as_mut(),
                &bytes.observations.reads,
                ErrorKind::Decode,
            )
            .await;
            assert_eq!(error.operation(), "decode_csv");
            assert!(std::error::Error::source(&error).is_some());
        }
    }
}

#[tokio::test]
async fn io_failure_after_a_delivered_row_does_not_read_ahead() {
    let bytes = MemoryAccessor::new(&fixture!("csv", "good_no_header.csv").await, 6)
        .with_failure(6, ErrorKind::Io);
    let accessor = CsvAccessor::new(bytes.clone(), schema());
    let mut reader = accessor
        .open(ReadRequest::new("data.csv").with_options(options(1, None)))
        .await
        .unwrap();
    assert_eq!(bytes.observations.reads.load(Ordering::SeqCst), 0);
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(collect_values::<Int64Type>(&[batch], 0), [1]);
    assert_eq!(bytes.observations.reads.load(Ordering::SeqCst), 1);
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Io);
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(bytes.observations.reads.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn header_validation_and_truncated_row_policy_are_explicit() {
    let accessor = CsvAccessor::new(
        MemoryAccessor::new(&fixture!("csv", "bad_header.csv").await, 1),
        schema(),
    )
    .with_format(
        CsvFormat::default()
            .with_header(true)
            .with_header_validation(true),
    );
    let mut reader = accessor.open(ReadRequest::new("header.csv")).await.unwrap();
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
    let accessor = CsvAccessor::new(
        MemoryAccessor::new(&fixture!("csv", "good_truncated_rows.csv").await, 1),
        schema(),
    )
    .with_format(CsvFormat::default().with_truncated_rows(true));
    let batches = collect(&accessor, ReadRequest::new("short.csv")).await;
    assert_eq!(collect_values::<Int64Type>(&batches, 0), [1, 2]);
    assert!(batches[0].column(1).is_null(0));
}

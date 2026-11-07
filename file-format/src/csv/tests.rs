use std::{
    num::NonZeroUsize,
    path::PathBuf,
    sync::{Arc, atomic::Ordering},
};

use arrow_array::{Array, Int64Array, StringArray};
use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use more_asserts::assert_gt;
use open_arrow_access_core::{
    Accessor, ByteReadOptions, ErrorKind, ReadOptions, ReadRequest, RecordBatch, SchemaRef,
};

use super::{CsvAccessor, CsvFormat};
use open_arrow_access_test_support::MemoryAccessor;

const BAD_FILES: [&str; 4] = [
    "bad_missing_column.csv",
    "bad_extra_column.csv",
    "bad_integer.csv",
    "bad_utf8.csv",
];

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../testdata/csv")
}

async fn fixture(name: &str) -> Vec<u8> {
    let path = fixture_root().join(name);
    tokio::fs::read(&path)
        .await
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

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
    accessor: &impl Accessor<Request = ReadRequest, Reader = dyn open_arrow_access_core::RecordReader>,
    request: ReadRequest,
) -> Vec<RecordBatch> {
    let mut reader = accessor.open(request).await.unwrap();
    let mut batches = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        assert_gt!(batch.num_rows(), 0);
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
    let input = fixture("good.csv").await;
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
    let accessor = CsvAccessor::new(MemoryAccessor::new(&fixture("good.tsv").await, 1), schema())
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
    let accessor = CsvAccessor::new(
        MemoryAccessor::new(&fixture("good_no_header.csv").await, 4),
        schema(),
    );
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
    let bytes = MemoryAccessor::new(&fixture("good_no_header.csv").await, 10);
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
async fn unsupported_csv_fields_are_configuration_errors_only_when_projected() {
    for data_type in [
        DataType::Binary,
        DataType::LargeUtf8,
        DataType::Duration(TimeUnit::Second),
        DataType::Interval(IntervalUnit::YearMonth),
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
    ] {
        let bytes = MemoryAccessor::new(&fixture("good_no_header.csv").await, 1);
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
        assert_eq!(ids(&batches, 0), [1, 2, 3]);
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
        let accessor = CsvAccessor::new(MemoryAccessor::new(&fixture(name).await, 1), schema())
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
    for name in BAD_FILES {
        let bytes = MemoryAccessor::new(&fixture(name).await, 1);
        let accessor = CsvAccessor::new(bytes.clone(), schema());
        let mut reader = accessor.open(ReadRequest::new(name)).await.unwrap();
        let error = reader.next().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Decode);
        assert_eq!(error.operation(), "decode_csv");
        assert!(std::error::Error::source(&error).is_some());
        let calls = bytes.observations.reads.load(Ordering::SeqCst);
        assert!(reader.next().await.unwrap().is_none());
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(bytes.observations.reads.load(Ordering::SeqCst), calls);
    }
}

#[tokio::test]
async fn csv_preserves_source_error_categories_and_stops_polling() {
    for kind in [
        ErrorKind::InvalidInput,
        ErrorKind::Unsupported,
        ErrorKind::NotFound,
        ErrorKind::PermissionDenied,
        ErrorKind::Io,
        ErrorKind::Decode,
    ] {
        for offset in [0, 6] {
            let bytes = MemoryAccessor::new(&fixture("good_no_header.csv").await, 6)
                .with_failure(offset, kind);
            let accessor = CsvAccessor::new(bytes.clone(), schema());
            let mut reader = accessor
                .open(ReadRequest::new("good_no_header.csv").with_options(options(1, None)))
                .await
                .unwrap();
            if offset != 0 {
                assert_eq!(ids(&[reader.next().await.unwrap().unwrap()], 0), [1]);
            }
            let error = reader.next().await.unwrap_err();
            assert_eq!(error.kind(), kind, "offset {offset}");
            assert_eq!(error.operation(), "memory_read");
            assert_eq!(
                std::error::Error::source(&error).unwrap().to_string(),
                "injected source failure"
            );
            let calls = bytes.observations.reads.load(Ordering::SeqCst);
            assert!(reader.next().await.unwrap().is_none());
            assert!(reader.next().await.unwrap().is_none());
            assert_eq!(bytes.observations.reads.load(Ordering::SeqCst), calls);
        }
    }
}

#[tokio::test]
async fn csv_rejects_empty_source_chunks_as_terminal_io_errors() {
    let bytes = MemoryAccessor::new(&fixture("good_no_header.csv").await, 1).with_empty_chunk(0);
    let accessor = CsvAccessor::new(bytes.clone(), schema());
    let mut reader = accessor
        .open(ReadRequest::new("good_no_header.csv"))
        .await
        .unwrap();
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Io);
    assert_eq!(error.operation(), "read_csv");
    let calls = bytes.observations.reads.load(Ordering::SeqCst);
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(bytes.observations.reads.load(Ordering::SeqCst), calls);
}

#[tokio::test]
async fn byte_options_are_forwarded_and_io_failures_preserve_kind_without_read_ahead() {
    let bytes =
        MemoryAccessor::new(&fixture("good_no_header.csv").await, 6).with_failure(6, ErrorKind::Io);
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
    let accessor = CsvAccessor::new(
        MemoryAccessor::new(&fixture("bad_header.csv").await, 1),
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
        MemoryAccessor::new(&fixture("good_truncated_rows.csv").await, 1),
        schema(),
    )
    .with_format(CsvFormat::default().with_truncated_rows(true));
    let batches = collect(&accessor, ReadRequest::new("short.csv")).await;
    assert_eq!(ids(&batches, 0), [1, 2]);
    assert!(batches[0].column(1).is_null(0));
}

#[tokio::test]
async fn filesystem_csv_preserves_values_across_chunks_and_batches() {
    use open_arrow_access_storage::OpenDalAccessor;

    for chunk_size in [1, 7, 4096] {
        let bytes =
            OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(fixture_root()))
                .with_chunk_size(NonZeroUsize::new(chunk_size).unwrap());
        let accessor = CsvAccessor::new(bytes, schema()).with_format(
            CsvFormat::default()
                .with_header(true)
                .with_header_validation(true),
        );
        let batches = collect(
            &accessor,
            ReadRequest::new("good.csv").with_options(options(2, None)),
        )
        .await;
        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            [2, 2, 1]
        );
        assert_eq!(ids(&batches, 0), [1, 2, 3, 4, 5]);
        let titles = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value.map(str::to_owned))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            titles,
            [
                Some("hello,world".into()),
                None,
                Some("two\nlines".into()),
                Some("say \"yes\"".into()),
                Some("last".into()),
            ]
        );
    }
}

#[tokio::test]
async fn filesystem_invalid_csv_errors_are_terminal() {
    use open_arrow_access_storage::OpenDalAccessor;

    for chunk_size in [1, 4096] {
        let bytes =
            OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(fixture_root()))
                .with_chunk_size(NonZeroUsize::new(chunk_size).unwrap());
        let accessor = CsvAccessor::new(bytes, schema());
        for name in BAD_FILES {
            let mut reader = accessor.open(ReadRequest::new(name)).await.unwrap();
            let error = reader.next().await.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Decode, "{name}");
            assert!(std::error::Error::source(&error).is_some(), "{name}");
            assert!(reader.next().await.unwrap().is_none(), "{name}");
        }
    }
}

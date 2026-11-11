use std::{
    num::NonZeroUsize,
    sync::{Arc, atomic::Ordering},
};

use arrow_array::{Array, Int64Array, StringArray, types::Int64Type};
use arrow_schema::{DataType, Field, Schema};
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteReadOptions, ErrorKind, ReadOptions, ReadRequest, RecordBatch,
    ScanPlanner, SchemaRef,
};

use futures::TryStreamExt;
use open_arrow_access_file_format::{
    DecodePartitionOptions,
    csv::{CsvAccessor, CsvDialect, CsvFormat},
};
use open_arrow_access_layers::ParallelReadLayer;
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
    let bytes = MemoryAccessor::new(&fixture!("csv", "good_no_header.csv").await, 1);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("title", DataType::Binary, true),
    ]));
    let accessor = CsvAccessor::new(bytes.clone(), schema);
    let error = accessor
        .open(ReadRequest::new("good_no_header.csv"))
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
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
        .with_dialect(CsvDialect {
            has_header: header,
            validate_header: header,
            ..CsvDialect::default()
        })
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ));
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
        let bytes = MemoryAccessor::new(&fixture!("csv", name).await, 1);
        let accessor = CsvAccessor::new(bytes.clone(), schema()).layer(ParallelReadLayer::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ));
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

#[tokio::test]
async fn io_failure_after_a_delivered_row_does_not_read_ahead() {
    let bytes = MemoryAccessor::new(&fixture!("csv", "good_no_header.csv").await, 6)
        .with_failure(6, ErrorKind::Io);
    let accessor = CsvAccessor::new(bytes.clone(), schema()).layer(ParallelReadLayer::new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    ));
    let request = ReadRequest::new("data.csv").with_options(options(1, None));
    let mut reader = accessor.open(request).await.unwrap();
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
    let accessor = CsvAccessor::new(MemoryAccessor::new(b"wrong,title", 1), schema())
        .with_dialect(CsvDialect {
            has_header: true,
            validate_header: true,
            ..CsvDialect::default()
        })
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ));
    let mut reader = accessor.open(ReadRequest::new("header.csv")).await.unwrap();
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
    let accessor = CsvAccessor::new(
        MemoryAccessor::new(&fixture!("csv", "good_truncated_rows.csv").await, 1),
        schema(),
    )
    .with_dialect(CsvDialect {
        allow_truncated_rows: true,
        ..CsvDialect::default()
    })
    .layer(ParallelReadLayer::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    ));
    let batches = collect(&accessor, ReadRequest::new("short.csv")).await;
    assert_eq!(collect_values::<Int64Type>(&batches, 0), [1, 2]);
    assert!(batches[0].column(1).is_null(0));
}

#[tokio::test]
async fn parallel_csv_preserves_multiline_quotes_comments_header_and_projection() {
    let raw = b"id,title\r\n1,\"a\nb\"\r\n#skip\r\n2,\"c\"\"d\"\r\n3,end";
    for projection in [Some(vec!["title", "id"]), Some(vec![])] {
        let accessor =
            CsvAccessor::new(MemoryAccessor::new(raw, 1), schema()).with_dialect(CsvDialect {
                has_header: true,
                validate_header: true,
                comment: Some(b'#'),
                ..CsvDialect::default()
            });
        let request = ReadRequest::new("multiline.csv").with_options(options(1, projection));
        let plan = accessor.plan_scan(request.clone()).await.unwrap();
        assert_eq!(
            plan.partitions.try_collect::<Vec<_>>().await.unwrap().len(),
            3
        );
        let mut expected = accessor.open(request.clone()).await.unwrap();
        let mut actual = accessor
            .layer(ParallelReadLayer::new(
                NonZeroUsize::new(3).unwrap(),
                NonZeroUsize::new(1).unwrap(),
            ))
            .open(request)
            .await
            .unwrap();
        loop {
            let batch = expected.next().await.unwrap();
            assert_eq!(actual.next().await.unwrap(), batch);
            if batch.is_none() {
                break;
            }
        }
    }
}

#[tokio::test]
async fn parallel_csv_strips_only_the_source_start_bom() {
    for raw in [
        b"\xef\xbb\xbf1,one\n2,two\n".as_slice(),
        b"1,one\n2,\xef\xbb\xbftwo\n",
    ] {
        // Use a complete source-start BOM for the native reference decoder.
        let expected = CsvAccessor::new(MemoryAccessor::new(raw, 4096), schema());
        let selection = ReadRequest::new("bom.csv").with_options(options(1, None));
        let expected = collect(&expected, selection.clone()).await;
        let mut actual = CsvAccessor::new(MemoryAccessor::new(raw, 1), schema())
            .layer(ParallelReadLayer::new(
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::new(1).unwrap(),
            ))
            .open(selection.clone())
            .await
            .unwrap();
        let mut batches = Vec::new();
        while let Some(batch) = actual.next().await.unwrap() {
            batches.push(batch);
        }
        assert_eq!(batches, expected);
    }
}

#[tokio::test]
async fn parallel_csv_custom_dialect_is_lazy_and_opaque_formats_remain_unsplit() {
    let input = MemoryAccessor::new(b"id;title|1;'a|b'|2;'c\\'d'|3;end", 1);
    let observations = input.observations.clone();
    let accessor = CsvAccessor::new(input, schema())
        .with_dialect(CsvDialect {
            has_header: true,
            validate_header: true,
            delimiter: b';',
            quote: b'\'',
            escape: Some(b'\\'),
            terminator: Some(b'|'),
            ..CsvDialect::default()
        })
        .with_partition_options(DecodePartitionOptions {
            target_bytes: NonZeroUsize::new(4).unwrap(),
        });
    let request = ReadRequest::new("custom.csv").with_options(options(10, None));
    let mut reader = accessor
        .clone()
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ))
        .open(request.clone())
        .await
        .unwrap();
    assert_eq!(observations.reads.load(Ordering::SeqCst), 0);
    // The soft byte target splits records even though the row target is ten.
    for (id, title) in [(1, "a|b"), (2, "c'd"), (3, "end")] {
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(
            collect_values::<Int64Type>(std::slice::from_ref(&batch), 0),
            [id]
        );
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            title
        );
    }
    assert!(reader.next().await.unwrap().is_none());
    let plan = accessor
        .with_format(CsvFormat::default())
        .plan_scan(request)
        .await
        .unwrap();
    assert_eq!(plan.partitions.size_hint().0, 1);
}

#[tokio::test]
async fn inference_detects_headers_replays_samples_and_supports_parallel_projection() {
    use open_arrow_access_file_format::csv::CsvInferenceOptions;
    use open_arrow_access_layers::ParallelReadLayer;
    for chunk_size in [1, 7, 4096] {
        for parallel in [false, true] {
            let bytes = fixture!("csv", "inference.csv").await;
            let input = MemoryAccessor::new(&bytes, chunk_size);
            let observed = input.observations.clone();
            let accessor = CsvAccessor::infer(input).with_inference_options(CsvInferenceOptions {
                max_records: 3,
                max_bytes: 1024,
                has_header: None,
            });
            let request = ReadRequest::new("records.csv").with_options(ReadOptions {
                batch_size: NonZeroUsize::new(1).unwrap(),
                projection: Some(vec![
                    "score".to_owned(),
                    "id".to_owned(),
                    "title".to_owned(),
                ]),
            });
            let mut reader = if parallel {
                accessor
                    .layer(ParallelReadLayer::new(
                        NonZeroUsize::new(2).unwrap(),
                        NonZeroUsize::new(1).unwrap(),
                    ))
                    .open(request)
                    .await
                    .unwrap()
            } else {
                accessor.open(request).await.unwrap()
            };
            let schema = reader.metadata().schema.clone();
            assert_eq!(schema.field(0).data_type(), &DataType::Float64);
            assert_eq!(schema.field(1).data_type(), &DataType::Int64);
            let mut ids = Vec::new();
            let mut titles = Vec::new();
            while let Some(batch) = reader.next().await.unwrap() {
                assert_eq!(batch.schema(), schema);
                ids.extend_from_slice(
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values(),
                );
                let values = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                titles.extend(values.iter().map(|value| value.unwrap().to_owned()));
            }
            assert_eq!(ids, [1, 2, 3]);
            assert_eq!(titles, ["first\nline", "second", "third"]);
            assert_eq!(observed.requests.lock().unwrap().len(), 1);
        }
    }
}

#[tokio::test]
async fn inference_detects_numeric_headerless_data_and_allows_string_header_override() {
    use open_arrow_access_file_format::csv::CsvInferenceOptions;
    for (bytes, has_header, column) in [
        (b"1,2\n3,4\n".as_slice(), None, "column_1"),
        (b"one,two\nthree,four\n", Some(false), "column_1"),
        (b"name,value\none,two\n", None, "name"),
    ] {
        let mut reader = CsvAccessor::infer(MemoryAccessor::new(bytes, 1))
            .with_inference_options(CsvInferenceOptions {
                has_header,
                ..Default::default()
            })
            .open(ReadRequest::new("records.csv"))
            .await
            .unwrap();
        assert_eq!(reader.metadata().schema.field(0).name(), column);
        let mut rows = 0;
        while let Some(batch) = reader.next().await.unwrap() {
            rows += batch.num_rows();
        }
        assert_eq!(rows, if column == "name" { 1 } else { 2 });
    }
}

#[tokio::test]
async fn inference_preserves_tsv_dialect_bom_gzip_and_zero_column_rows() {
    use open_arrow_access_decompression::{DecompressionLayer, gzip::Gzip};
    use open_arrow_access_file_format::csv::CsvDialect;
    use std::io::Write;
    let mut encoded = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoded
        .write_all(b"\xef\xbb\xbfid\ttitle\r\n1\t'first\tvalue'\r\n2\t'next'\r\n")
        .unwrap();
    let bytes = encoded.finish().unwrap();
    let input = MemoryAccessor::new(&bytes, 1).layer(DecompressionLayer::new(Gzip));
    let accessor = CsvAccessor::infer(input).with_dialect(CsvDialect {
        delimiter: b'\t',
        quote: b'\'',
        has_header: true,
        ..Default::default()
    });
    let mut reader = accessor
        .open(ReadRequest::new("records.tsv").with_options(ReadOptions {
            projection: Some(vec![]),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert!(reader.metadata().schema.fields().is_empty());
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        rows += batch.num_rows();
    }
    assert_eq!(rows, 2);
}

#[tokio::test]
async fn inference_rejects_empty_header_only_duplicate_and_null_columns() {
    for bytes in [
        b"".as_slice(),
        b"id,title\n",
        b"id,id\n1,2\n",
        b"id,empty\n1,\n2,\n",
    ] {
        assert!(
            CsvAccessor::infer(MemoryAccessor::new(bytes, 1))
                .open(ReadRequest::new("records.csv"))
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn inference_limits_validate_before_io_and_later_type_conflicts_are_terminal() {
    use open_arrow_access_file_format::csv::CsvInferenceOptions;
    let input = MemoryAccessor::new(b"id\n1\n", 1);
    let observed = input.observations.clone();
    let accessor = CsvAccessor::infer(input).with_inference_options(CsvInferenceOptions {
        max_records: 0,
        ..Default::default()
    });
    assert!(
        accessor
            .open(ReadRequest::new("records.csv"))
            .await
            .is_err()
    );
    assert_eq!(observed.requests.lock().unwrap().len(), 0);
    let input = MemoryAccessor::new(b"1\n2\nbad\n", 1);
    let mut reader = CsvAccessor::infer(input)
        .with_inference_options(CsvInferenceOptions {
            max_records: 2,
            max_bytes: 1,
            has_header: Some(false),
        })
        .open(ReadRequest::new("records.csv").with_options(ReadOptions {
            batch_size: NonZeroUsize::new(1).unwrap(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        reader.metadata().schema.field(0).data_type(),
        &DataType::Int64
    );
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
    assert!(reader.next().await.is_err());
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn inference_propagates_source_errors_and_rejects_opaque_formats_before_io() {
    let input = MemoryAccessor::new(b"id\n1\n", 1).with_failure(2, ErrorKind::PermissionDenied);
    assert!(
        CsvAccessor::infer(input)
            .open(ReadRequest::new("records.csv"))
            .await
            .is_err()
    );
    let input = MemoryAccessor::new(b"id\n1\n", 1);
    let observed = input.observations.clone();
    let accessor = CsvAccessor::infer(input).with_format(CsvFormat::default().with_header(true));
    assert!(
        accessor
            .open(ReadRequest::new("records.csv"))
            .await
            .is_err()
    );
    assert_eq!(observed.requests.lock().unwrap().len(), 0);
}

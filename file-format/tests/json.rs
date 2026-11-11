use futures::TryStreamExt;
use open_arrow_access_decompression::{DecompressionLayer, gzip::Gzip};
use open_arrow_access_file_format::DecodePartitionOptions;
use open_arrow_access_layers::ParallelReadLayer;
use std::io::Write;
use std::{
    error::Error as _,
    num::NonZeroUsize,
    sync::{Arc, atomic::Ordering},
};

use arrow_array::{Array, Int64Array, StringArray};
use arrow_schema::{ArrowError, DataType, Field, IntervalUnit, Schema, TimeUnit};
use more_asserts::assert_lt;
use open_arrow_access_core::{
    Accessor, AccessorExt, ErrorKind, ReadOptions, ReadRequest, RecordBatch, ScanPlanner, SchemaRef,
};

use open_arrow_access_file_format::json::JsonAccessor;
use open_arrow_access_test_support::{MemoryAccessor, Observations, fixture, format};

fn schema() -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("title", DataType::Utf8, true),
        ],
        [("fixture".to_owned(), "json".to_owned())].into(),
    ))
}

async fn fixture(name: &str, chunk_size: usize) -> (MemoryAccessor, Arc<Observations>) {
    let input = MemoryAccessor::new(&fixture!("json", name).await, chunk_size);
    let observations = input.observations.clone();
    (input, observations)
}

fn request(batch_size: usize, projection: Option<Vec<&str>>) -> ReadRequest {
    ReadRequest::new("records.json").with_options(ReadOptions {
        batch_size: NonZeroUsize::new(batch_size).unwrap(),
        projection: projection.map(|names| names.into_iter().map(str::to_owned).collect()),
    })
}

fn ids(batch: &RecordBatch) -> &[i64] {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
}

#[tokio::test]
async fn fragmented_json_objects_preserve_values_schema_and_batch_order() {
    for chunk_size in [1, 7, 4096] {
        let (input, _) = fixture("good.json", chunk_size).await;
        let accessor = JsonAccessor::new(input, schema());
        let mut reader = accessor.open(request(2, None)).await.unwrap();
        assert_eq!(reader.metadata().schema, schema());
        assert_eq!(
            reader.metadata().source.as_ref().unwrap().locator(),
            "records.json"
        );
        let first = reader.next().await.unwrap().unwrap();
        assert_eq!(first.schema(), schema());
        assert_eq!(ids(&first), [0, 1]);
        let titles = first
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(titles.value(0), "雪\nline");
        assert!(titles.is_null(1));
        let last = reader.next().await.unwrap().unwrap();
        assert_eq!(ids(&last), [2, 3]);
        assert!(last.column(1).is_null(0));
        assert!(reader.next().await.unwrap().is_none());
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn fixed_schema_contract_preserves_projection_and_source_failures() {
    let input = fixture!("json", "projection.jsonl").await;
    let batches = format::assert_fixed_schema_contract(
        |bytes| JsonAccessor::new(bytes, schema()),
        format::FormatCase {
            bytes: &input,
            request: request(1, Some(vec!["title", "id"])),
            schema: schema(),
            rows: 2,
            fault_offsets: &[
                0,
                5,
                input.iter().position(|&byte| byte == b'\n').unwrap() + 1,
            ],
        },
    )
    .await;
    for (batch, id) in batches.iter().zip([1, 2]) {
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            id
        );
    }
}

#[tokio::test]
async fn malformed_truncated_and_non_object_inputs_fail_terminally() {
    // One representative per parser failure, plus a projected-away syntax error.
    // Scalar variants and every projection crossed with every malformed fixture
    // repeat Arrow's validation rather than exercising another adapter path.
    for (name, projection) in [
        ("bad-empty-array.json", None),
        ("bad-null.json", None),
        ("bad-truncated-object.json", None),
        ("bad-trailing.json", None),
        ("bad-type.json", None),
        ("bad-utf8.json", None),
        ("bad-trailing.json", Some(vec![])),
    ] {
        let (input, observed) = fixture(name, 1).await;
        let accessor = JsonAccessor::new(input, schema());
        let mut reader = accessor.open(request(10, projection)).await.unwrap();
        let error = reader
            .next()
            .await
            .err()
            .unwrap_or_else(|| panic!("accepted {name}"));
        assert_eq!(error.kind(), ErrorKind::Decode, "{name}");
        assert_eq!(error.operation(), "decode_json", "{name}");
        assert!(error.source().unwrap().is::<ArrowError>(), "{name}");
        format::assert_finished(reader.as_mut(), &observed.reads).await;
    }
}

#[tokio::test]
async fn reading_a_batch_does_not_collect_the_entire_input() {
    let data = fixture!("json", "many.jsonl").await;
    let input = MemoryAccessor::new(&data, 1);
    let observed = input.observations.clone();
    let accessor = JsonAccessor::new(input, schema());
    let mut reader = accessor.open(request(2, None)).await.unwrap();
    assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
    assert_eq!(ids(&reader.next().await.unwrap().unwrap()), [0, 1]);
    assert_lt!(observed.reads.load(Ordering::SeqCst), data.len());
}

#[tokio::test]
async fn empty_and_whitespace_only_inputs_have_no_rows() {
    for name in ["empty.json", "whitespace.json"] {
        let (input, observed) = fixture(name, 1).await;
        let accessor = JsonAccessor::new(input, schema());
        let mut reader = accessor.open(request(1, None)).await.unwrap();
        assert!(reader.next().await.unwrap().is_none());
        format::assert_finished(reader.as_mut(), &observed.reads).await;
    }
}

#[tokio::test]
async fn decoder_configuration_errors_are_classified_before_opening_bytes() {
    for (data_type, expected) in [
        (
            DataType::Interval(IntervalUnit::DayTime),
            ErrorKind::Unsupported,
        ),
        (
            DataType::Timestamp(TimeUnit::Millisecond, Some("invalid/timezone".into())),
            ErrorKind::InvalidInput,
        ),
    ] {
        let (input, observed) = fixture("empty.json", 1).await;
        let schema = Arc::new(Schema::new(vec![Field::new("value", data_type, true)]));
        let accessor = JsonAccessor::new(input, schema);
        let error = accessor.open(request(1, None)).await.err().unwrap();
        assert_eq!(error.kind(), expected);
        assert_eq!(error.operation(), "open_json");
        assert!(error.source().unwrap().is::<ArrowError>());
        assert!(observed.requests.lock().unwrap().is_empty());
        assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn parallel_object_chunks_preserve_pretty_json_and_gzip_projection() {
    let raw = br#"{ "id": 1, "title": "brace } and \"quote\"" }
    {
      "id": 2, "title": "nested"
    } {"id":3,"title":null}"#;
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gzip.write_all(raw).unwrap();
    let bytes = gzip.finish().unwrap();
    for projection in [None, Some(vec!["title", "id"]), Some(vec![])] {
        let accessor = JsonAccessor::new(
            MemoryAccessor::new(&bytes, 1).layer(DecompressionLayer::new(Gzip)),
            schema(),
        );
        let selection = request(1, projection);
        let plan = accessor.plan_scan(selection.clone()).await.unwrap();
        assert_eq!(
            plan.partitions.try_collect::<Vec<_>>().await.unwrap().len(),
            3
        );
        let mut expected = accessor.open(selection.clone()).await.unwrap();
        let mut actual = accessor
            .layer(ParallelReadLayer::new(
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::new(1).unwrap(),
            ))
            .open(selection)
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
async fn parallel_json_reports_truncated_tail_and_accepts_records_larger_than_target() {
    let accessor = JsonAccessor::new(MemoryAccessor::new(b"{\"id\":1}{\"id\":", 1), schema());
    let mut reader = accessor
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ))
        .open(request(1, None))
        .await
        .unwrap();
    assert!(reader.next().await.unwrap().is_some());
    assert!(reader.next().await.is_err());
    let accessor = JsonAccessor::new(MemoryAccessor::new(b"{\"id\":1}", 1), schema())
        .with_partition_options(DecodePartitionOptions {
            target_bytes: NonZeroUsize::new(2).unwrap(),
        });
    let mut reader = accessor
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ))
        .open(request(1, None))
        .await
        .unwrap();
    assert_eq!(ids(&reader.next().await.unwrap().unwrap()), [1]);
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn parallel_json_framing_handles_nested_values_and_rejects_invalid_syntax() {
    let raw = format!(
        "  {{\"id\":1,\"title\":\"{}é\",\"extra\":[{{\"x\":\"}}\\\"\"}},null]}}\n\t",
        "x".repeat(16 * 1024)
    );
    for chunk_size in [7, 4096] {
        let accessor = JsonAccessor::new(MemoryAccessor::new(raw.as_bytes(), chunk_size), schema())
            .with_partition_options(DecodePartitionOptions {
                target_bytes: NonZeroUsize::new(32).unwrap(),
            });
        let mut expected = accessor.open(request(1, None)).await.unwrap();
        let mut actual = accessor
            .layer(ParallelReadLayer::new(
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::new(1).unwrap(),
            ))
            .open(request(1, None))
            .await
            .unwrap();
        assert_eq!(actual.next().await.unwrap(), expected.next().await.unwrap());
        assert!(actual.next().await.unwrap().is_none());
    }
    for raw in [b"{\"id\":1,}".as_slice(), b"{\"id\":[}", b"[]", b"null"] {
        let accessor = JsonAccessor::new(MemoryAccessor::new(raw, 1), schema());
        let error = accessor
            .plan_scan(request(1, None))
            .await
            .unwrap()
            .partitions
            .try_collect::<Vec<_>>()
            .await
            .err()
            .expect("invalid JSON must fail framing");
        assert_eq!(error.kind(), ErrorKind::Decode);
    }
}

#[tokio::test]
async fn inference_deduces_nested_types_and_replays_sampled_records() {
    use open_arrow_access_file_format::json::JsonInferenceOptions;
    let bytes = fixture!("json", "inference.jsonl").await;
    for chunk_size in [1, 7, 4096] {
        let input = MemoryAccessor::new(&bytes, chunk_size);
        let mut reader = JsonAccessor::infer(input)
            .with_inference_options(JsonInferenceOptions {
                max_records: 2,
                max_bytes: 1024,
            })
            .open(request(1, None))
            .await
            .unwrap();
        let schema = reader.metadata().schema.clone();
        assert_eq!(
            schema.field_with_name("id").unwrap().data_type(),
            &DataType::Int64
        );
        let DataType::List(item) = schema.field_with_name("values").unwrap().data_type() else {
            panic!("expected an inferred list");
        };
        assert_eq!(item.data_type(), &DataType::Int64);
        let DataType::Struct(fields) = schema.field_with_name("nested").unwrap().data_type() else {
            panic!("expected an inferred struct");
        };
        assert_eq!(fields[0].name(), "name");
        assert_eq!(fields[0].data_type(), &DataType::Utf8);
        let extra = schema.field_with_name("extra").unwrap();
        assert_eq!(extra.data_type(), &DataType::Boolean);
        assert!(extra.is_nullable());
        let mut output_ids = Vec::new();
        let mut nulls = 0;
        while let Some(batch) = reader.next().await.unwrap() {
            assert_eq!(batch.schema(), schema);
            output_ids.extend_from_slice(
                batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values(),
            );
            nulls += batch.column_by_name("extra").unwrap().null_count();
        }
        assert_eq!(output_ids, [1, 2, 3]);
        assert_eq!(nulls, 2);
    }
}

#[tokio::test]
async fn inference_requires_a_nonempty_sample_and_positive_limits() {
    use open_arrow_access_file_format::json::JsonInferenceOptions;
    assert!(
        JsonAccessor::infer(MemoryAccessor::new(b"", 1))
            .open(request(2, None))
            .await
            .is_err()
    );
    let input = MemoryAccessor::new(b"{}", 1);
    let observed = input.observations.clone();
    let accessor = JsonAccessor::infer(input).with_inference_options(JsonInferenceOptions {
        max_records: 0,
        max_bytes: 1,
    });
    assert!(accessor.open(request(2, None)).await.is_err());
    assert_eq!(observed.requests.lock().unwrap().len(), 0);
}

#[tokio::test]
async fn inference_fixes_the_schema_at_record_and_byte_sample_limits() {
    use open_arrow_access_file_format::json::JsonInferenceOptions;
    for options in [
        JsonInferenceOptions {
            max_records: 1,
            max_bytes: 1024,
        },
        JsonInferenceOptions {
            max_records: 1000,
            max_bytes: 1,
        },
    ] {
        for chunk_size in [1, 4096] {
            let input = MemoryAccessor::new(b"{\"id\":1} {\"id\":\"bad\"}", chunk_size);
            let mut reader = JsonAccessor::infer(input)
                .with_inference_options(options)
                .open(request(1, None))
                .await
                .unwrap();
            assert_eq!(
                reader.metadata().schema.field(0).data_type(),
                &DataType::Int64
            );
            assert_eq!(ids(&reader.next().await.unwrap().unwrap()), [1]);
            assert!(reader.next().await.is_err());
        }
    }
}

#[tokio::test]
async fn nonfinite_values_survive_fragmentation_and_parallel_decoding() {
    use arrow_array::{
        cast::AsArray,
        types::{Float32Type, Float64Type},
    };

    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Float64, true),
        Field::new(
            "nested",
            DataType::Struct(
                vec![Field::new(
                    "values",
                    DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                    true,
                )]
                .into(),
            ),
            true,
        ),
        Field::new("NaN", DataType::Utf8, true),
    ]));
    let raw = br#"{"value":NaN,"nested":{"values":[Infinity,-Infinity,null]},"NaN":"NaN Infinity -Infinity \""}
    {"value":Infinity} {"value":-Infinity} {"value":1.5} {"value":null} {"value":-1.5,"extra":"\\NaN"}"#;
    for chunk_size in [1, 7, 4096] {
        for parallel in [false, true] {
            let accessor = JsonAccessor::new(MemoryAccessor::new(raw, chunk_size), schema.clone());
            let mut reader = if parallel {
                accessor
                    .layer(ParallelReadLayer::new(
                        NonZeroUsize::new(2).unwrap(),
                        NonZeroUsize::MIN,
                    ))
                    .open(request(2, None))
                    .await
                    .unwrap()
            } else {
                accessor.open(request(2, None)).await.unwrap()
            };
            let first = reader.next().await.unwrap().unwrap();
            let values = first.column(0).as_primitive::<Float64Type>();
            assert!(values.value(0).is_nan());
            assert_eq!(values.value(1), f64::INFINITY);
            let nested = first.column(1).as_struct();
            let lists = nested.column(0).as_list::<i32>();
            let list = lists.value(0);
            let items = list.as_primitive::<Float32Type>();
            assert_eq!(items.value(0), f32::INFINITY);
            assert_eq!(items.value(1), f32::NEG_INFINITY);
            assert!(items.is_null(2));
            let strings = first.column(2).as_string::<i32>();
            assert_eq!(strings.value(0), "NaN Infinity -Infinity \"");
            let second = reader.next().await.unwrap().unwrap();
            let values = second.column(0).as_primitive::<Float64Type>();
            assert_eq!(values.values().as_ref(), &[f64::NEG_INFINITY, 1.5]);
            let last = reader.next().await.unwrap().unwrap();
            let values = last.column(0).as_primitive::<Float64Type>();
            assert!(values.is_null(0));
            assert_eq!(values.value(1), -1.5);
            assert!(reader.next().await.unwrap().is_none());
        }
    }
}

#[tokio::test]
async fn nonfinite_compatibility_does_not_accept_malformed_json() {
    for raw in [
        br#"{"value":NaNx}"#.as_slice(),
        br#"{"value":-NaN}"#,
        br#"{NaN :2}"#,
        br#"{"value":1,NaN :2}"#,
        br#"{"value":Infinity:1}"#,
        br#"{"value":NaN,"bad":}"#,
        br#"{"value":NaN} trailing"#,
        br#"{"value":Inf"#,
    ] {
        for parallel in [false, true] {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Float64,
                true,
            )]));
            let accessor = JsonAccessor::new(MemoryAccessor::new(raw, 1), schema);
            let mut reader = if parallel {
                accessor
                    .layer(ParallelReadLayer::new(NonZeroUsize::MIN, NonZeroUsize::MIN))
                    .open(request(2, None))
                    .await
                    .unwrap()
            } else {
                accessor.open(request(2, None)).await.unwrap()
            };
            loop {
                match reader.next().await {
                    Ok(Some(_)) => continue,
                    Ok(None) => panic!("accepted {raw:?}"),
                    Err(error) => {
                        assert_eq!(error.kind(), ErrorKind::Decode);
                        break;
                    }
                }
            }
            assert!(reader.next().await.unwrap().is_none());
        }
    }
}

#[tokio::test]
async fn inference_deduces_float_types_for_bare_nonfinite_values_only() {
    let input = MemoryAccessor::new(
        br#"{"value":NaN,"nested":{"value":Infinity},"text":"NaN"} {"value":-Infinity}"#,
        1,
    );
    let mut reader = JsonAccessor::infer(input)
        .open(request(1, None))
        .await
        .unwrap();
    let schema = reader.metadata().schema.clone();
    assert_eq!(
        schema.field_with_name("value").unwrap().data_type(),
        &DataType::Float64
    );
    let DataType::Struct(fields) = schema.field_with_name("nested").unwrap().data_type() else {
        panic!("expected a struct");
    };
    assert_eq!(fields[0].data_type(), &DataType::Float64);
    assert_eq!(
        schema.field_with_name("text").unwrap().data_type(),
        &DataType::Utf8
    );
    let batch = reader.next().await.unwrap().unwrap();
    assert!(
        batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap()
            .value(0)
            .is_nan()
    );
}

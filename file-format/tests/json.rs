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
            fault_offsets: &[0, 5],
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
    for name in [
        "bad-empty-array.json",
        "bad-object-array.json",
        "bad-null.json",
        "bad-number.json",
        "bad-boolean.json",
        "bad-string.json",
        "bad-truncated-value.json",
        "bad-truncated-object.json",
        "bad-trailing.json",
        "bad-type.json",
        "bad-utf8.json",
    ] {
        for projection in [None, Some(vec![])] {
            // A projected-away field's value is deliberately not decoded.
            if matches!(name, "bad-type.json" | "bad-utf8.json") && projection.is_some() {
                continue;
            }
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
}

#[tokio::test]
async fn io_errors_keep_their_category_after_a_successful_batch() {
    let (input, observed) = fixture("io-failure.jsonl", 9).await;
    let input = input.with_failure(9, ErrorKind::Io);
    let accessor = JsonAccessor::new(input, schema());
    let mut reader = accessor.open(request(1, None)).await.unwrap();
    assert_eq!(ids(&reader.next().await.unwrap().unwrap()), [1]);
    let error =
        format::assert_terminal_error(reader.as_mut(), &observed.reads, ErrorKind::Io).await;
    assert_eq!(error.operation(), "memory_read");
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

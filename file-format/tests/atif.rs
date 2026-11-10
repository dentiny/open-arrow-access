use std::{error::Error as _, num::NonZeroUsize, sync::atomic::Ordering};

use arrow_array::{RecordBatch, cast::AsArray};
use atif_arrow::trajectory_schema;
use open_arrow_access_core::{Accessor, ByteReadOptions, ErrorKind, ReadOptions, ReadRequest};
use open_arrow_access_file_format::atif::{AtifAccessor, AtifFormat};
use open_arrow_access_test_support::{MemoryAccessor, fixture, format};

fn text<'a>(batch: &'a RecordBatch, column: &str, row: usize) -> &'a str {
    batch[column].as_string::<i32>().value(row)
}

#[tokio::test]
async fn fixed_schema_nested_content_and_verbatim_source_across_single_byte_chunks() {
    let input = fixture!("atif", "trajectory.json").await;
    let bytes = MemoryAccessor::new(&input, 1).with_ranges().with_listing();
    let observations = bytes.observations.clone();
    let accessor = AtifAccessor::new(bytes, AtifFormat::Json);
    let capability = accessor.capability();
    assert!(capability.read);
    assert!(!capability.read_with_range && !capability.read_parallel && !capability.list);
    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        io_chunk_size: NonZeroUsize::new(7),
    };
    let mut reader = accessor
        .open(ReadRequest::new("trajectory.json").with_byte_options(options))
        .await
        .unwrap();
    assert_eq!(observations.reads.load(Ordering::SeqCst), 0);
    assert_eq!(observations.requests.lock().unwrap()[0].options, options);
    assert_eq!(reader.capability(), capability);
    assert_eq!(reader.metadata().schema, trajectory_schema());
    assert_eq!(
        reader.metadata().source.as_ref().unwrap().locator(),
        "trajectory.json"
    );
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.schema(), reader.metadata().schema);
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(text(&batch, "raw_json", 0).as_bytes(), input);
    assert_eq!(text(&batch, "source_uri", 0), "trajectory.json");
    assert_eq!(
        batch["source_record_index"]
            .as_primitive::<arrow_array::types::UInt64Type>()
            .value(0),
        1
    );
    let steps = batch["steps"].as_list::<i32>().value(0);
    let steps = steps.as_struct();
    let message = steps["message"].as_list::<i32>().value(0);
    assert_eq!(
        message.as_struct()["text"].as_binary::<i64>().value(0),
        "你好🙂".as_bytes()
    );
    let calls = steps["tool_calls"].as_list::<i32>().value(1);
    assert_eq!(
        calls.as_struct()["function_name"]
            .as_string::<i32>()
            .value(0),
        "echo"
    );
    assert!(reader.next().await.unwrap().is_none());
    format::assert_finished(reader.as_mut(), &observations.reads).await;
}

#[tokio::test]
async fn jsonl_batching_projection_and_document_indices_preserve_order() {
    let input = fixture!("atif", "trajectories.jsonl").await;
    for chunk_size in [1, 17, input.len()] {
        let accessor = AtifAccessor::new(
            MemoryAccessor::new(&input, chunk_size),
            AtifFormat::JsonLines,
        );
        let mut reader = accessor
            .open(ReadRequest::new("runs.jsonl").with_options(ReadOptions {
                batch_size: NonZeroUsize::new(2).unwrap(),
                projection: Some(vec![
                    "trajectory_id".into(),
                    "source_record_index".into(),
                    "raw_json".into(),
                ]),
            }))
            .await
            .unwrap();
        let mut ids = Vec::new();
        let mut indices = Vec::new();
        let mut sizes = Vec::new();
        while let Some(batch) = reader.next().await.unwrap() {
            assert_eq!(batch.schema(), reader.metadata().schema);
            assert_eq!(batch.schema().field(0).name(), "trajectory_id");
            sizes.push(batch.num_rows());
            for row in 0..batch.num_rows() {
                ids.push(text(&batch, "trajectory_id", row).to_owned());
                indices.push(
                    batch["source_record_index"]
                        .as_primitive::<arrow_array::types::UInt64Type>()
                        .value(row),
                );
                assert!(text(&batch, "raw_json", row).contains("\"unknown\":{\"preserved\":true}"));
            }
        }
        assert_eq!(ids, ["first", "second", "third"]);
        assert_eq!(indices, [1, 2, 3]);
        assert_eq!(sizes, [2, 1]);
    }
}

#[tokio::test]
async fn fixed_schema_contract_preserves_projection_and_source_failures() {
    format::assert_fixed_schema_contract(
        |bytes| AtifAccessor::new(bytes, AtifFormat::Json),
        format::FormatCase {
            bytes: &fixture!("atif", "trajectory.json").await,
            request: ReadRequest::new("x").with_options(ReadOptions {
                projection: Some(vec!["source_record_index".into(), "trajectory_id".into()]),
                ..ReadOptions::default()
            }),
            schema: trajectory_schema(),
            rows: 1,
            fault_offsets: &[0, 8],
        },
    )
    .await;
}

#[tokio::test]
async fn malformed_documents_utf8_and_invalid_atif_are_terminal_with_source_context() {
    for name in [
        "bad_truncated.jsonl",
        "bad_array.jsonl",
        "bad_utf8.jsonl",
        "bad_missing_fields.jsonl",
        "bad_version.jsonl",
    ] {
        let input = fixture!("atif", name).await;
        let bytes = MemoryAccessor::new(&input, 3);
        let observations = bytes.observations.clone();
        let mut reader = AtifAccessor::new(bytes, AtifFormat::JsonLines)
            .open(ReadRequest::new("broken.jsonl").with_options(ReadOptions {
                // Valid row 1 must be discarded with the invalid row in this batch.
                batch_size: NonZeroUsize::new(2).unwrap(),
                projection: Some(vec![]),
            }))
            .await
            .unwrap();
        let error =
            format::assert_terminal_error(reader.as_mut(), &observations.reads, ErrorKind::Decode)
                .await;
        assert!(
            error.to_string().contains("broken.jsonl: record 2:"),
            "{error}"
        );
        assert!(error.source().is_some());
    }
}

#[tokio::test]
async fn jsonl_empty_input_is_eof_but_json_requires_one_document() {
    for name in ["empty.jsonl", "whitespace.jsonl"] {
        let input = fixture!("atif", name).await;
        let mut reader = AtifAccessor::new(MemoryAccessor::new(&input, 1), AtifFormat::JsonLines)
            .open(ReadRequest::new("empty"))
            .await
            .unwrap();
        assert!(reader.next().await.unwrap().is_none());
        let mut reader = AtifAccessor::new(MemoryAccessor::new(&input, 1), AtifFormat::Json)
            .open(ReadRequest::new("empty"))
            .await
            .unwrap();
        assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
    }
}

#[tokio::test]
async fn jsonl_pulls_only_the_requested_batch_and_keeps_delivered_rows_on_later_error() {
    let input = fixture!("atif", "bad_late_record.jsonl").await;
    let first_line_bytes = input.iter().position(|&byte| byte == b'\n').unwrap() + 1;
    let bytes = MemoryAccessor::new(&input, 1);
    let observations = bytes.observations.clone();
    let mut reader = AtifAccessor::new(bytes, AtifFormat::JsonLines)
        .open(ReadRequest::new("runs.jsonl").with_options(ReadOptions {
            batch_size: NonZeroUsize::MIN,
            projection: Some(vec!["trajectory_id".into()]),
        }))
        .await
        .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(text(&batch, "trajectory_id", 0), "first");
    assert_eq!(observations.reads.load(Ordering::SeqCst), first_line_bytes);
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Decode);
    assert!(error.to_string().contains("record 2:"));
    format::assert_finished(reader.as_mut(), &observations.reads).await;
    assert_eq!(text(&batch, "trajectory_id", 0), "first");
}

use arrow_array::{Array, RecordBatch, cast::AsArray, types::UInt64Type};
use open_arrow_access_core::{Accessor, ReadOptions, ReadRequest, Reader, ScanPlanner};
use open_arrow_access_file_format::DecodePartitionOptions;
use open_arrow_access_service::semantic_scholar::{S2orcV2Accessor, s2orc_v2_schema};
use open_arrow_access_test_support::MemoryAccessor;
use serde_json::{Value, json};
use std::num::NonZeroUsize;

fn record(id: u64) -> Value {
    json!({"corpusid": id, "title": "A paper", "authors": ["Author A"],
      "openaccessinfo": {"externalids": {"DOI": "10.example/test", "ArXiv": null}, "license": "CCBY"},
      "body": {"text": "Hello world", "annotations": {
        "sentence": "[{\"start\":\"0\",\"end\":\"11\",\"attributes\":null}]",
        "paragraph": "[]", "section_header": null,
        "bib_ref": "[{\"start\":0,\"end\":5,\"attributes\":{\"ref_id\":\"b0\",\"matched_paper_id\":42}}]"}},
      "bibliography": {"text": "Reference", "annotations": {"bib_entry": "[]"}}})
}
async fn batches(
    raw: &[u8],
    projection: Option<Vec<String>>,
) -> open_arrow_access_core::Result<Vec<RecordBatch>> {
    let request = ReadRequest::new("part-00000").with_options(ReadOptions {
        projection,
        batch_size: NonZeroUsize::new(2).unwrap(),
    });
    let mut reader = S2orcV2Accessor::new(MemoryAccessor::new(raw, 1))
        .open(request)
        .await?;
    let mut out = vec![];
    while let Some(batch) = reader.next().await? {
        out.push(batch);
    }
    assert!(reader.next().await?.is_none());
    Ok(out)
}

#[tokio::test]
async fn normalizes_nested_payloads_without_json_string_columns() {
    let out = batches(record(1).to_string().as_bytes(), None)
        .await
        .unwrap();
    let b = &out[0];
    assert_eq!(b.schema(), s2orc_v2_schema());
    assert_eq!(b["corpusid"].as_primitive::<UInt64Type>().value(0), 1);
    let annotations = b["body"]
        .as_struct()
        .column_by_name("annotations")
        .unwrap()
        .as_struct();
    let sentence = annotations
        .column_by_name("sentence")
        .unwrap()
        .as_list::<i32>();
    let value = sentence.value(0);
    assert_eq!(
        value
            .as_struct()
            .column_by_name("end")
            .unwrap()
            .as_primitive::<UInt64Type>()
            .value(0),
        11
    );
    assert_eq!(
        value
            .as_struct()
            .column_by_name("attributes")
            .unwrap()
            .null_count(),
        1
    );
    assert_eq!(
        annotations
            .column_by_name("paragraph")
            .unwrap()
            .as_list::<i32>()
            .value_length(0),
        0
    );
    assert!(
        annotations
            .column_by_name("section_header")
            .unwrap()
            .is_null(0)
    );
    assert_eq!(
        b["authors"]
            .as_list::<i32>()
            .value(0)
            .as_string::<i32>()
            .value(0),
        "Author A"
    );
}

#[tokio::test]
async fn projection_preserves_rows_and_validates_unselected_annotations() {
    for columns in [vec!["title".into(), "corpusid".into()], vec![]] {
        let out = batches(record(1).to_string().as_bytes(), Some(columns.clone()))
            .await
            .unwrap();
        assert_eq!(out[0].num_rows(), 1);
        assert_eq!(out[0].num_columns(), columns.len());
    }
    let mut row = record(1);
    row["body"]["annotations"]["sentence"] = json!("not JSON");
    assert!(
        batches(row.to_string().as_bytes(), Some(vec!["title".into()]))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn scan_chunks_cover_rows_once_and_match_sequential_schema() {
    let raw = (1..=5)
        .map(|id| record(id).to_string())
        .collect::<Vec<_>>()
        .join("\n");
    let reader = S2orcV2Accessor::new(MemoryAccessor::new(raw.as_bytes(), 7))
        .with_partition_options(DecodePartitionOptions {
            target_bytes: NonZeroUsize::new(64).unwrap(),
        });
    let plan = reader
        .plan_scan(ReadRequest::new("part-00000"))
        .await
        .unwrap();
    assert!(plan.streaming_discovery);
    assert_eq!(plan.metadata.schema, s2orc_v2_schema());
    let mut planned = plan.into_reader();
    let mut ids: Vec<u64> = vec![];
    while let Some(b) = planned.next().await.unwrap() {
        ids.extend(b["corpusid"].as_primitive::<UInt64Type>().values());
    }
    assert_eq!(ids, vec![1_u64, 2, 3, 4, 5]);
}

#[tokio::test]
async fn empty_shards_retain_schema_and_truncated_input_fails() {
    assert!(batches(b"", None).await.unwrap().is_empty());
    assert!(batches(b"{\"corpusid\":1", None).await.is_err());
    let raw = json!({"corpusid": 1}).to_string();
    let out = batches(raw.as_bytes(), None).await.unwrap();
    assert!(out[0]["body"].is_null(0));
}

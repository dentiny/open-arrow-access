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

#[tokio::test]
async fn rejects_unknown_fields_at_every_object_level_even_when_projected_out() {
    for path in [
        "",
        "/openaccessinfo",
        "/openaccessinfo/externalids",
        "/body",
        "/body/annotations",
        "/bibliography",
        "/bibliography/annotations",
    ] {
        let mut row = record(1);
        row.pointer_mut(path)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("unexpected".into(), json!("must not disappear"));
        assert!(
            batches(row.to_string().as_bytes(), Some(vec!["corpusid".into()]))
                .await
                .is_err(),
            "accepted unknown field at {path}"
        );
    }
    for span in [
        json!({"start":0,"end":1,"future":true}),
        json!({"start":0,"end":1,"attributes":{"future":true}}),
    ] {
        let mut row = record(1);
        row["body"]["annotations"]["sentence"] = json!(json!([span]).to_string());
        assert!(
            batches(row.to_string().as_bytes(), Some(vec![]))
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn rejects_type_drift_and_invalid_identity() {
    for (path, value) in [
        ("/corpusid", json!(null)),
        ("/corpusid", json!(0)),
        ("/corpusid", json!(-1)),
        ("/title", json!(123)),
        ("/authors", json!([true])),
        ("/authors", json!([null])),
        ("/openaccessinfo/license", json!(true)),
        ("/openaccessinfo/externalids/DOI", json!(123)),
        ("/body/text", json!({"not":"text"})),
        ("/body/annotations/sentence", json!([])),
    ] {
        let mut row = record(1);
        *row.pointer_mut(path).unwrap() = value;
        assert!(
            batches(row.to_string().as_bytes(), None).await.is_err(),
            "accepted invalid value at {path}"
        );
    }
}

#[tokio::test]
async fn validates_span_payloads_and_text_dependency() {
    for payload in [
        "null",
        "{}",
        "[null]",
        "[{\"start\":2,\"end\":1}]",
        "[{\"start\":\"-1\",\"end\":1}]",
        "[{\"start\":\"18446744073709551616\",\"end\":1}]",
        "[{\"start\":0.5,\"end\":1}]",
        "[{\"end\":1}]",
    ] {
        let mut row = record(1);
        row["body"]["annotations"]["sentence"] = json!(payload);
        assert!(
            batches(row.to_string().as_bytes(), None).await.is_err(),
            "accepted payload {payload}"
        );
    }
    let mut row = record(1);
    row["body"]["text"] = Value::Null;
    assert!(batches(row.to_string().as_bytes(), None).await.is_err());
}

#[tokio::test]
async fn all_annotation_kinds_and_external_ids_have_fixed_types_after_null_rows() {
    let mut row = record(2);
    row["openaccessinfo"]["externalids"] = json!({"Medline":"123v1", "MAG":"123", "ACL":"P00-0001", "DOI":"10.example/x", "MedRxiv":"identifier", "PubMedCentral":"123", "ArXiv":"1234.56789"});
    for (section, kinds) in [
        (
            "body",
            vec!["sentence", "paragraph", "section_header", "bib_ref"],
        ),
        (
            "bibliography",
            vec![
                "bib_entry",
                "bib_id",
                "bib_title",
                "bib_venue",
                "bib_author_first_name",
                "bib_author_last_name",
            ],
        ),
    ] {
        for kind in kinds {
            row[section]["annotations"][kind] = json!(
                "[{\"start\":0,\"end\":1,\"attributes\":{\"n\":\"1\",\"id\":\"b0\",\"ref_id\":\"b0\",\"matched_paper_id\":42}}]"
            );
        }
    }
    let raw = format!("{}\n{}", json!({"corpusid":1}), row);
    let out = batches(raw.as_bytes(), None).await.unwrap();
    assert_eq!(out[0].schema(), s2orc_v2_schema());
    assert_eq!(out[0].num_rows(), 2);
    assert!(out[0]["body"].is_null(0));
    assert!(out[0]["body"].is_valid(1));
}

/// Explicit full-input audit, usable against a decompressed local release shard.
/// No schema inference or row sampling is performed. No file bytes enter the repo.
#[tokio::test]
#[ignore = "set S2ORC_V2_SHARD_PATH to a decompressed JSONL file"]
async fn audit_local_shard() {
    let path = std::path::PathBuf::from(std::env::var("S2ORC_V2_SHARD_PATH").unwrap());
    let bytes = open_arrow_access_storage::OpenDalAccessor::new(
        open_arrow_access_test_support::fs_operator(path.parent().unwrap()),
    );
    let mut reader = S2orcV2Accessor::new(bytes)
        .open(ReadRequest::new(
            path.file_name().unwrap().to_str().unwrap(),
        ))
        .await
        .unwrap();
    let mut rows = 0_u64;
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.schema(), s2orc_v2_schema());
        rows += batch.num_rows() as u64;
    }
    println!("validated {rows} typed S2ORC v2 rows");
}

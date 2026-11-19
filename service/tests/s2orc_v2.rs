use arrow_array::{Array, RecordBatch, cast::AsArray, types::UInt64Type};
use futures::TryStreamExt;
use open_arrow_access_core::{Accessor, ReadOptions, ReadRequest, ScanPlanner, into_stream};
use open_arrow_access_file_format::DecodePartitionOptions;
use open_arrow_access_service::semantic_scholar::{S2orcV2Accessor, s2orc_v2_schema};
use open_arrow_access_test_support::{MemoryAccessor, fixture};
use serde_json::{Value, json};
use std::num::NonZeroUsize;

async fn batches(
    raw: &[u8],
    projection: Option<Vec<String>>,
) -> open_arrow_access_core::Result<Vec<RecordBatch>> {
    let request = ReadRequest::new("part-00000").with_options(ReadOptions {
        projection,
        batch_size: NonZeroUsize::new(2).unwrap(),
    });
    let reader = S2orcV2Accessor::new(MemoryAccessor::new(raw, 4096))
        .open(request)
        .await?;
    into_stream(reader).try_collect().await
}

fn rows(batches: &[RecordBatch]) -> Vec<RecordBatch> {
    batches
        .iter()
        .flat_map(|batch| (0..batch.num_rows()).map(|row| batch.slice(row, 1)))
        .collect()
}

#[tokio::test]
async fn normalizes_nested_payloads_without_json_string_columns() {
    let raw = fixture!("semantic-scholar", "s2orc_v2-synthetic.json").await;
    let out = batches(&raw, None).await.unwrap();
    let b = &out[0];
    assert_eq!(b.schema(), s2orc_v2_schema());
    assert_eq!(b["corpusid"].as_primitive::<UInt64Type>().value(0), 1);
    let annotations = b["body"].as_struct()["annotations"].as_struct();
    let sentence = annotations["sentence"].as_list::<i32>();
    let value = sentence.value(0);
    assert_eq!(
        value.as_struct()["end"]
            .as_primitive::<UInt64Type>()
            .value(0),
        11
    );
    assert_eq!(value.as_struct()["attributes"].null_count(), 1);
    assert_eq!(annotations["paragraph"].as_list::<i32>().value_length(0), 0);
    assert!(annotations["section_header"].is_null(0));
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
async fn validates_unselected_annotations() {
    let invalid = fixture!("semantic-scholar", "s2orc_v2-invalid-annotation.json").await;
    assert!(batches(&invalid, Some(vec!["title".into()])).await.is_err());
}

#[tokio::test]
async fn scan_chunks_cover_rows_once_and_match_sequential_schema() {
    let raw = fixture!("semantic-scholar", "s2orc_v2-sample.jsonl").await;
    let sequential = batches(&raw, None).await.unwrap();
    let reader = S2orcV2Accessor::new(MemoryAccessor::new(&raw, 4096)).with_partition_options(
        DecodePartitionOptions {
            target_bytes: NonZeroUsize::new(64).unwrap(),
        },
    );
    let plan = reader
        .plan_scan(ReadRequest::new("part-00000"))
        .await
        .unwrap();
    assert!(plan.streaming_discovery);
    assert_eq!(plan.metadata.schema, s2orc_v2_schema());
    let planned = into_stream(Box::new(plan.into_reader()))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let actual = rows(&planned);
    let expected = rows(&sequential);
    assert_eq!(actual, expected);
    assert_eq!(actual.len(), 20);
}

#[tokio::test]
async fn empty_shards_retain_schema_and_truncated_input_fails() {
    let empty = fixture!("semantic-scholar", "s2orc_v2-empty.jsonl").await;
    assert!(batches(&empty, None).await.unwrap().is_empty());
    let truncated = fixture!("semantic-scholar", "s2orc_v2-truncated.json").await;
    assert!(batches(&truncated, None).await.is_err());
    let raw = fixture!("semantic-scholar", "s2orc_v2-minimal.json").await;
    let out = batches(&raw, None).await.unwrap();
    assert!(out[0]["body"].is_null(0));
}

#[tokio::test]
async fn decodes_official_public_sample() {
    let raw = fixture!("semantic-scholar", "s2orc_v2-sample.jsonl").await;
    let out = batches(&raw, None).await.unwrap();
    assert_eq!(out.iter().map(RecordBatch::num_rows).sum::<usize>(), 20);
    for batch in &out {
        assert_eq!(batch.schema(), s2orc_v2_schema());
    }
    let first = &out[0];
    assert_eq!(
        first["corpusid"].as_primitive::<UInt64Type>().value(0),
        281539311
    );
    assert_eq!(
        first["title"].as_string::<i64>().value(0),
        "Post-Heroin Post-Traumatic Stress Disorder Spectrum: Heroin Addiction as a Generator of Trauma Sensitisation in Everyday Life: A Perspective Review"
    );
    let authors = first["authors"].as_list::<i32>().value(0);
    assert_eq!(authors.as_string::<i32>().value(0), "I. Maremmani");
    let body = first["body"].as_struct();
    assert!(
        body["text"]
            .as_string::<i64>()
            .value(0)
            .starts_with("\nIntroduction\n\nPost-Traumatic Stress Disorder (PTSD)")
    );
    let annotations = body["annotations"].as_struct();
    for (name, count) in [
        ("sentence", 344),
        ("paragraph", 101),
        ("section_header", 22),
        ("bib_ref", 246),
    ] {
        assert_eq!(annotations[name].as_list::<i32>().value(0).len(), count);
    }
    for columns in [vec!["title".into(), "corpusid".into()], vec![]] {
        let projected = batches(&raw, Some(columns.clone())).await.unwrap();
        assert_eq!(
            projected.iter().map(RecordBatch::num_rows).sum::<usize>(),
            20
        );
        for batch in projected {
            assert_eq!(batch.num_columns(), columns.len());
        }
    }
}

async fn valid_record() -> Value {
    serde_json::from_slice(&fixture!("semantic-scholar", "s2orc_v2-synthetic.json").await).unwrap()
}

// Empty projection still validates the complete input record.
async fn assert_rejected(row: Value) {
    assert!(
        batches(row.to_string().as_bytes(), Some(vec![]))
            .await
            .is_err(),
        "accepted {row}"
    );
}

#[tokio::test]
async fn rejects_unknown_fields_even_when_projected_out() {
    let base = valid_record().await;
    for path in [
        "",
        "/openaccessinfo",
        "/openaccessinfo/externalids",
        "/body",
        "/body/annotations",
        "/bibliography",
        "/bibliography/annotations",
    ] {
        let mut row = base.clone();
        row.pointer_mut(path).unwrap()["unexpected"] = json!(true);
        assert_rejected(row).await;
    }
}

#[tokio::test]
async fn rejects_invalid_values_even_when_projected_out() {
    let base = valid_record().await;
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
        ("/body/text", json!(null)),
        ("/body/annotations/sentence", json!([])),
    ] {
        let mut row = base.clone();
        *row.pointer_mut(path).unwrap() = value;
        assert_rejected(row).await;
    }
    for payload in [
        "null",
        "{}",
        "[null]",
        r#"[{"start":2,"end":1}]"#,
        r#"[{"start":"-1","end":1}]"#,
        r#"[{"start":"18446744073709551616","end":1}]"#,
        r#"[{"start":0.5,"end":1}]"#,
        r#"[{"end":1}]"#,
        r#"[{"start":0,"end":1,"future":true}]"#,
        r#"[{"start":0,"end":1,"attributes":{"future":true}}]"#,
    ] {
        let mut row = base.clone();
        row["body"]["annotations"]["sentence"] = json!(payload);
        assert_rejected(row).await;
    }
}

#[tokio::test]
async fn all_annotation_kinds_and_external_ids_have_fixed_types_after_null_rows() {
    let mut row = valid_record().await;
    row["openaccessinfo"]["externalids"] = json!({"Medline":"123v1", "MAG":"123", "ACL":"P00-0001", "DOI":"10.example/x", "MedRxiv":"identifier", "PubMedCentral":"123", "ArXiv":"1234.56789"});
    let span = json!([{"start":0,"end":1,"attributes":{"n":"1","id":"b0","ref_id":"b0","matched_paper_id":42}}]).to_string();
    for (section, kinds) in [
        ("body", "sentence paragraph section_header bib_ref"),
        (
            "bibliography",
            "bib_entry bib_id bib_title bib_venue bib_author_first_name bib_author_last_name",
        ),
    ] {
        for kind in kinds.split_whitespace() {
            row[section]["annotations"][kind] = json!(span);
        }
    }
    let raw = format!("{}\n{}", json!({"corpusid":2}), row);
    let out = batches(raw.as_bytes(), None).await.unwrap();
    assert_eq!(out[0].schema(), s2orc_v2_schema());
    assert_eq!(out[0].num_rows(), 2);
    assert!(out[0]["body"].is_null(0));
    assert!(out[0]["body"].is_valid(1));
}

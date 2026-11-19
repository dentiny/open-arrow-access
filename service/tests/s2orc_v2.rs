use arrow_array::{Array, RecordBatch, cast::AsArray, types::UInt64Type};
use futures::TryStreamExt;
use open_arrow_access_core::{Accessor, ReadOptions, ReadRequest, ScanPlanner, into_stream};
use open_arrow_access_file_format::DecodePartitionOptions;
use open_arrow_access_service::semantic_scholar::{S2orcV2Accessor, s2orc_v2_schema};
use open_arrow_access_test_support::{MemoryAccessor, fixture};
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

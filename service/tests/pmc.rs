use std::num::NonZeroUsize;

use arrow_array::{Array, cast::AsArray};
use open_arrow_access_core::{
    Accessor, AccessorExt, ErrorKind, ReadOptions, ReadRequest, RecordBatch, Result,
};
use open_arrow_access_decompression::{DecompressionLayer, Gzip};
use open_arrow_access_service::pmc::{PmcAccessor, PmcLimits, schema};
use open_arrow_access_test_support::MemoryAccessor;

const RAW: &str = include_str!("../../testdata/pmc/metadata.json");

async fn read(raw: &[u8], projection: Option<Vec<String>>) -> Result<RecordBatch> {
    let mut reader = PmcAccessor::new(MemoryAccessor::new(raw, 1))
        .open(ReadRequest::new("a.json").with_options(ReadOptions {
            projection,
            ..ReadOptions::default()
        }))
        .await?;
    reader
        .next()
        .await
        .map(|batch| batch.expect("one metadata row"))
}

#[tokio::test]
async fn maps_metadata_and_preserves_raw_json_without_fetching_content() {
    let source = MemoryAccessor::new(RAW.as_bytes(), 1);
    let observed = source.observations.clone();
    let mut reader = PmcAccessor::new(source)
        .open(ReadRequest::new("a.json"))
        .await
        .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.schema(), schema());
    assert_eq!(batch.num_rows(), 1);
    for (field, expected) in [
        ("pmcid", " pmc00123 "),
        ("pmid", "36537061"),
        ("license_code", "CC BY"),
    ] {
        assert_eq!(batch[field].as_string::<i32>().value(0), expected);
    }
    assert!(batch["is_pmc_openaccess"].as_boolean().value(0));
    assert_eq!(batch["raw_metadata_json"].as_string::<i64>().value(0), RAW);
    assert_eq!(observed.requests.lock().unwrap().len(), 1);
    let batch = read(
        br#"{"pmcid":"PMC1","license_code":"TDM","is_manuscript":true}"#,
        None,
    )
    .await
    .unwrap();
    assert!(batch["pdf_url"].is_null(0));
}

#[tokio::test]
async fn projection_preserves_order_and_zero_column_rows() {
    for names in [vec!["pdf_url".into(), "pmcid".into()], vec![]] {
        let batch = read(RAW.as_bytes(), Some(names.clone())).await.unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>(),
            names,
        );
    }
}

#[tokio::test]
async fn media_lists_preserve_null_empty_and_populated_values() {
    for (value, expected) in [("null", None), ("[]", Some(0)), (r#"["a", "b"]"#, Some(2))] {
        let raw = format!(r#"{{"pmcid":"PMC1","media_urls":{value}}}"#);
        let batch = read(raw.as_bytes(), None).await.unwrap();
        let urls = batch["media_urls"].as_list::<i32>();
        assert_eq!(urls.is_null(0), expected.is_none());
        if let Some(length) = expected {
            assert_eq!(urls.value_length(0), length);
        }
    }
}

#[tokio::test]
async fn validates_metadata_even_when_all_fields_are_projected_away() {
    let inputs: &[&[u8]] = &[
        b"",
        b"[]",
        b"{",
        b"{}",
        b"\xff",
        br#"{"pmcid":"PMC0"}"#,
        br#"{"pmcid":"PMC1","pmcid":null}"#,
        br#"{"pmcid":"PMC1","version":0}"#,
        br#"{"pmcid":"PMC1","version":4294967296}"#,
        br#"{"pmcid":"PMC1","doi":42}"#,
        br#"{"pmcid":"PMC1","is_manuscript":"true"}"#,
        br#"{"pmcid":"PMC1","media_urls":[null]}"#,
    ];
    for raw in inputs {
        assert_eq!(
            read(raw, Some(vec![])).await.unwrap_err().kind(),
            ErrorKind::Decode,
            "{raw:?}"
        );
    }
}

#[tokio::test]
async fn limits_apply_to_decoded_metadata() {
    for limit in [RAW.len(), RAW.len() - 1] {
        let compressed =
            MemoryAccessor::new(include_bytes!("../../testdata/pmc/metadata.json.gz"), 1);
        let mut reader = PmcAccessor::new(compressed.layer(DecompressionLayer::new(Gzip)))
            .with_limits(PmcLimits {
                metadata_bytes: NonZeroUsize::new(limit).unwrap(),
            })
            .open(ReadRequest::new("a.json"))
            .await
            .unwrap();
        let result = reader.next().await;
        if limit == RAW.len() {
            assert_eq!(result.unwrap().unwrap().num_rows(), 1);
        } else {
            assert_eq!(result.unwrap_err().kind(), ErrorKind::Decode);
        }
    }
}

#[tokio::test]
async fn duplicate_fields_use_the_final_value_and_preserve_empty_strings() {
    let batch = read(
        br#"{"pmcid":"PMC1","pmid":1,"pmid":"00001","doi":""}"#,
        None,
    )
    .await
    .unwrap();
    assert_eq!(batch["pmid"].as_string::<i32>().value(0), "00001");
    assert_eq!(batch["doi"].as_string::<i32>().value(0), "");
}

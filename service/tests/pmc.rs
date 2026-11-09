use arrow_array::{Array, cast::AsArray};
use open_arrow_access_core::{
    Accessor, AccessorExt, ErrorKind, ReadOptions, ReadRequest, RecordBatch, Result,
};
use open_arrow_access_decompression::{DecompressionLayer, gzip::Gzip};
use open_arrow_access_service::pmc::{PmcAccessor, schema};
use open_arrow_access_test_support::{MemoryAccessor, fixture};

const RAW: &str = include_str!("../../testdata/pmc/metadata.json");

async fn read(raw: &[u8], projection: Option<Vec<String>>) -> Result<RecordBatch> {
    let mut reader = PmcAccessor::with_default_decoder(MemoryAccessor::new(raw, 1))
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
async fn maps_metadata_without_fetching_content() {
    let source = MemoryAccessor::new(RAW.as_bytes(), 1);
    let observed = source.observations.clone();
    let mut reader = PmcAccessor::with_default_decoder(source)
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
    assert_eq!(observed.requests.lock().unwrap().len(), 1);
    let batch = read(&fixture!("pmc", "metadata_only.json").await, None)
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
    for (name, expected) in [
        ("media_null.json", None),
        ("media_empty.json", Some(0)),
        ("media_populated.json", Some(2)),
    ] {
        let raw = fixture!("pmc", name).await;
        let batch = read(&raw, None).await.unwrap();
        let urls = batch["media_urls"].as_list::<i32>();
        assert_eq!(urls.is_null(0), expected.is_none());
        if let Some(length) = expected {
            assert_eq!(urls.value_length(0), length);
        }
    }
}

#[tokio::test]
async fn validates_metadata_even_when_all_fields_are_projected_away() {
    for name in [
        "bad_empty.json",
        "bad_array.json",
        "bad_truncated.json",
        "bad_missing_pmcid.json",
        "bad_utf8.json",
        "bad_pmcid.json",
        "bad_duplicate_pmcid.json",
        "bad_zero_version.json",
        "bad_overflow_version.json",
        "bad_doi.json",
        "bad_manuscript.json",
        "bad_media.json",
    ] {
        let raw = fixture!("pmc", name).await;
        assert_eq!(
            read(&raw, Some(vec![])).await.unwrap_err().kind(),
            ErrorKind::Decode,
            "{name}"
        );
    }
}

#[tokio::test]
async fn reads_decoded_metadata_without_a_byte_limit() {
    let compressed = MemoryAccessor::new(include_bytes!("../../testdata/pmc/metadata.json.gz"), 1);
    let mut reader =
        PmcAccessor::with_default_decoder(compressed.layer(DecompressionLayer::new(Gzip)))
            .open(ReadRequest::new("a.json"))
            .await
            .unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
    let mut large = vec![b' '; 4 * 1024 * 1024 + 1];
    large.extend_from_slice(RAW.as_bytes());
    let mut reader = PmcAccessor::with_default_decoder(MemoryAccessor::new(&large, 64 * 1024))
        .open(ReadRequest::new("large.json"))
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
}

#[tokio::test]
async fn duplicate_fields_use_the_final_value_and_preserve_empty_strings() {
    let batch = read(&fixture!("pmc", "duplicate_fields.json").await, None)
        .await
        .unwrap();
    assert_eq!(batch["pmid"].as_string::<i32>().value(0), "00001");
    assert_eq!(batch["doi"].as_string::<i32>().value(0), "");
}

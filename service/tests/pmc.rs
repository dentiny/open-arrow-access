use arrow_array::{Array, cast::AsArray, types::UInt64Type};
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
    assert_eq!(batch["content_url"], batch["pdf_url"]);
    let metadata = batch["metadata"].as_struct();
    assert_eq!(metadata.num_columns(), 16);
    assert_eq!(
        metadata
            .column_by_name("mid")
            .unwrap()
            .as_string::<i32>()
            .value(0),
        "NIHMS123"
    );
    assert_eq!(
        metadata
            .column_by_name("pmid")
            .unwrap()
            .as_primitive::<UInt64Type>()
            .value(0),
        36537061
    );
    assert_eq!(
        metadata.column_by_name("pdf_url").unwrap(),
        &batch["pdf_url"]
    );
    assert!(batch.schema().field_with_name("metadata_json").is_err());
    for (field, expected) in [("pmcid", " pmc00123 "), ("license_code", "CC BY")] {
        assert_eq!(batch[field].as_string::<i32>().value(0), expected);
    }
    assert_eq!(
        batch["pmid"].as_primitive::<UInt64Type>().value(0),
        36537061
    );
    assert!(batch["is_pmc_openaccess"].as_boolean().value(0));
    assert_eq!(observed.requests.lock().unwrap().len(), 1);
    let batch = read(&fixture!("pmc", "metadata_only.json").await, None)
        .await
        .unwrap();
    assert!(batch["pdf_url"].is_null(0));
    assert!(batch["pmid"].is_null(0));
    assert!(batch["text"].is_null(0));
    let batch = read(
        br#"{"pmcid":"PMC1","text_url":"s3://pmc-oa-opendata/PMC1.1/article%20name.txt?md5=opaque"}"#,
        Some(vec!["text".into()]),
    ).await.unwrap();
    assert_eq!(batch.schema().field(0).name(), "text");
    assert_eq!(
        batch["text"].as_string::<i32>().value(0),
        "s3://pmc-oa-opendata/PMC1.1/article%20name.txt?md5=opaque"
    );
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
async fn unknown_metadata_fields_fail_instead_of_being_dropped() {
    let raw = br#"{"pmcid":"PMC1","future_field":{"a":1}}"#;
    for projection in [None, Some(vec!["metadata".into()]), Some(vec![])] {
        assert_eq!(
            read(raw, projection).await.unwrap_err().kind(),
            ErrorKind::Decode
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
    assert_eq!(batch["pmid"].as_primitive::<UInt64Type>().value(0), 1);
    assert_eq!(batch["doi"].as_string::<i32>().value(0), "");
}

mod discovery {
    use super::*;
    use open_arrow_access_service::pmc::{BUCKET_URL, PmcDatasetAccessor, bucket_operator};
    use open_arrow_access_storage::OpenDalAccessor;

    #[tokio::test]
    async fn reads_only_metadata_from_release_or_subset() {
        let root = tempfile::tempdir().unwrap();
        let operator = open_arrow_access_test_support::fs_operator(root.path());
        for (path, contents) in [
            ("metadata/PMC1.json", r#"{"pmcid":"PMC1"}"#),
            ("metadata/subset/PMC2.json", r#"{"pmcid":"PMC2"}"#),
            ("metadata/article.pdf", "not JSON"),
        ] {
            operator.write(path, contents).await.unwrap();
        }
        let source = PmcDatasetAccessor::new(OpenDalAccessor::new(operator));
        for (path, expected) in [
            ("metadata/", vec!["PMC1", "PMC2"]),
            ("metadata/subset/", vec!["PMC2"]),
            ("metadata/PMC1.json", vec!["PMC1"]),
            ("empty/", vec![]),
        ] {
            let mut reader = source
                .open(ReadRequest::new(path).with_options(ReadOptions {
                    projection: Some(vec!["pmcid".into()]),
                    ..ReadOptions::default()
                }))
                .await
                .unwrap();
            let mut ids = Vec::new();
            while let Some(batch) = reader.next().await.unwrap() {
                assert_eq!(batch.num_columns(), 1);
                ids.push(batch["pmcid"].as_string::<i32>().value(0).to_owned());
            }
            ids.sort();
            assert_eq!(ids, expected);
        }
    }

    #[tokio::test]
    async fn public_operator_reads_bucket_root_without_signing() {
        use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter, OperationContext};
        struct Unsigned;
        impl HttpTransport for Unsigned {
            async fn fetch(
                &self,
                request: http::Request<Buffer>,
            ) -> opendal::Result<http::Response<HttpBody>> {
                assert_eq!(request.uri().host(), Some("s3.us-east-1.amazonaws.com"));
                assert_eq!(
                    request.uri().path(),
                    "/pmc-oa-opendata/PMC123.1/article.pdf"
                );
                assert!(!request.headers().contains_key("authorization"));
                assert!(!request.headers().contains_key("x-amz-security-token"));
                Ok(open_arrow_access_test_support::http::response(
                    http::StatusCode::OK,
                    "PDF",
                ))
            }
        }
        assert_eq!(BUCKET_URL, "s3://pmc-oa-opendata/");
        let operator = bucket_operator().unwrap().with_context(
            OperationContext::new().with_http_transport(HttpTransporter::new(Unsigned)),
        );
        assert_eq!(operator.info().root(), "/");
        assert_eq!(
            operator
                .read("PMC123.1/article.pdf")
                .await
                .unwrap()
                .to_bytes(),
            "PDF"
        );
    }
}

#[tokio::test]
async fn content_format_selects_one_url_without_fetching_or_falling_back() {
    use open_arrow_access_service::pmc::{PmcDatasetAccessor, PmcDecoder, PmcFormat};
    let raw = br#"{"pmcid":"PMC1","pdf_url":"s3://bucket/paper.pdf?md5=pdf","xml_url":"s3://bucket/paper.xml?md5=xml","text_url":"s3://bucket/paper.txt?md5=txt"}"#;
    for (format, expected) in [
        (PmcFormat::Pdf, "s3://bucket/paper.pdf?md5=pdf"),
        (PmcFormat::Xml, "s3://bucket/paper.xml?md5=xml"),
        (PmcFormat::Txt, "s3://bucket/paper.txt?md5=txt"),
    ] {
        assert_eq!(format.to_string().parse::<PmcFormat>().unwrap(), format);
        let storage = MemoryAccessor::new(raw, 1);
        let observed = storage.observations.clone();
        let mut direct = PmcAccessor::new(storage.clone(), PmcDecoder::new(format))
            .open(ReadRequest::new("metadata/a.json"))
            .await
            .unwrap();
        let batch = direct.next().await.unwrap().unwrap();
        assert_eq!(batch["content_url"].as_string::<i32>().value(0), expected);
        assert_eq!(batch["metadata"].as_struct().num_columns(), 16);
        let mut dataset = PmcDatasetAccessor::new(storage)
            .with_format(format)
            .open(
                ReadRequest::new("metadata/a.json")
                    .with_options(ReadOptions::default().with_projection(["content_url"])),
            )
            .await
            .unwrap();
        let batch = dataset.next().await.unwrap().unwrap();
        assert_eq!(batch["content_url"].as_string::<i32>().value(0), expected);
        assert_eq!(observed.requests.lock().unwrap().len(), 2);
    }
    let batch = read(
        br#"{"pmcid":"PMC1","xml_url":"s3://bucket/paper.xml"}"#,
        None,
    )
    .await
    .unwrap();
    assert!(batch["content_url"].is_null(0)); // Default PDF never falls back to XML.
    assert!("html".parse::<PmcFormat>().is_err());
}

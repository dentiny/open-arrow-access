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
    assert!(batch["text"].is_null(0));
    let batch = read(
        br#"{"pmcid":"PMC1","text_url":"s3://pmc-oa-opendata/PMC1.1/article%20name.txt?md5=opaque","text":"ignored"}"#,
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

mod discovery {
    use super::*;
    use futures::{StreamExt, TryStreamExt, stream};
    use open_arrow_access_core::{
        ByteAccessor, ByteReadOptions, ByteReadRequest, ByteReader, Capability, Error, ListRequest,
        ObjectStream, ScanPlanner, async_trait,
    };
    use open_arrow_access_layers::ParallelReadLayer;
    use open_arrow_access_service::pmc::{
        BUCKET_URL, PmcDatasetAccessor, PmcRequest, bucket_operator,
    };
    use open_arrow_access_storage::OpenDalAccessor;
    use std::{
        num::NonZeroUsize,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    #[derive(Clone, Default)]
    struct Listing {
        requests: Arc<Mutex<Vec<ListRequest>>>,
        polled: Arc<AtomicUsize>,
        bytes: Option<MemoryAccessor>,
        fail: bool,
        outside: bool,
    }

    #[async_trait]
    impl Accessor for Listing {
        type Request = ByteReadRequest;
        type Reader = dyn ByteReader;
        fn capability(&self) -> Capability {
            Capability {
                read: true,
                list: true,
                ..Capability::default()
            }
        }
        async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
            self.bytes.as_ref().unwrap().open(request).await
        }
    }

    #[async_trait]
    impl ByteAccessor for Listing {
        async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
            self.requests.lock().unwrap().push(request.clone());
            let polled = self.polled.clone();
            let fail = self.fail;
            let paths = if self.outside {
                vec!["other/PMC1.json".to_owned()]
            } else {
                [
                    "PMC1.json",
                    "nested/PMC2.json",
                    "article.pdf",
                    "inventory.csv.gz",
                    "README",
                    "upper.JSON",
                ]
                .map(|key| format!("{}{key}", request.prefix.locator()))
                .to_vec()
            };
            let stream = stream::iter(paths.into_iter().enumerate()).map(move |(index, path)| {
                polled.fetch_add(1, Ordering::SeqCst);
                if fail && index == 1 {
                    Err(Error::new(
                        ErrorKind::PermissionDenied,
                        "fixture_list",
                        "denied",
                    ))
                } else {
                    Ok(path.into())
                }
            });
            Ok(stream.boxed())
        }
    }

    #[tokio::test]
    async fn discovery_is_recursive_filtered_and_lazy() {
        let listing = Listing::default();
        let source = PmcDatasetAccessor::new(listing.clone());
        let mut objects = source.metadata_paths(&PmcRequest::release()).await.unwrap();
        assert_eq!(listing.polled.load(Ordering::SeqCst), 0);
        assert_eq!(
            objects.try_next().await.unwrap().unwrap().locator(),
            "metadata/PMC1.json"
        );
        assert_eq!(listing.polled.load(Ordering::SeqCst), 1);
        assert_eq!(objects.try_collect::<Vec<_>>().await.unwrap().len(), 1);
        let requests = listing.requests.lock().unwrap();
        assert_eq!(requests[0].prefix.locator(), "metadata/");
        assert!(requests[0].recursive);
    }

    #[tokio::test]
    async fn reads_release_subprefix_and_empty_mirror_without_content() {
        let root = tempfile::tempdir().unwrap();
        let operator = open_arrow_access_test_support::fs_operator(root.path());
        operator
            .write("metadata/PMC1.json", br#"{"pmcid":"PMC1"}"#.as_slice())
            .await
            .unwrap();
        operator
            .write(
                "metadata/nested/PMC2.json",
                br#"{"pmcid":"PMC2"}"#.as_slice(),
            )
            .await
            .unwrap();
        // These would fail JSON decoding if discovery accidentally included them.
        operator
            .write("metadata/README.txt", "ignored")
            .await
            .unwrap();
        operator
            .write("metadata/PMC1.pdf", "ignored")
            .await
            .unwrap();
        let source = PmcDatasetAccessor::new(OpenDalAccessor::new(operator));
        for (request, expected) in [
            (PmcRequest::release(), 2),
            (PmcRequest::prefix("metadata/nested/"), 1),
            (PmcRequest::prefix("absent/"), 0),
            (PmcRequest::files(Vec::<String>::new()), 0),
        ] {
            let mut reader = source.open(request).await.unwrap();
            assert_eq!(reader.metadata().schema, schema());
            let mut count = 0;
            while let Some(batch) = reader.next().await.unwrap() {
                count += batch.num_rows();
            }
            assert_eq!(count, expected);
        }
    }

    #[tokio::test]
    async fn explicit_subset_needs_no_listing_and_preserves_options_and_duplicates() {
        let storage = MemoryAccessor::new(RAW.as_bytes(), 64);
        let observed = storage.observations.clone();
        let source = PmcDatasetAccessor::new(storage);
        let byte_options = ByteReadOptions {
            io_concurrency: NonZeroUsize::new(3).unwrap(),
            ..ByteReadOptions::default()
        };
        let request = PmcRequest::files([
            "metadata/PMC2.json",
            "metadata/PMC1.json",
            "metadata/PMC2.json",
        ])
        .with_options(ReadOptions {
            projection: Some(vec![]),
            ..ReadOptions::default()
        })
        .with_byte_options(byte_options);
        let mut reader = source.open(request).await.unwrap();
        assert!(observed.requests.lock().unwrap().is_empty());
        for _ in 0..3 {
            let batch = reader.next().await.unwrap().unwrap();
            assert_eq!((batch.num_rows(), batch.num_columns()), (1, 0));
        }
        assert!(reader.next().await.unwrap().is_none());
        let requests = observed.requests.lock().unwrap();
        assert_eq!(
            requests
                .iter()
                .map(|r| r.object.locator())
                .collect::<Vec<_>>(),
            [
                "metadata/PMC2.json",
                "metadata/PMC1.json",
                "metadata/PMC2.json"
            ]
        );
        assert!(requests.iter().all(|r| r.options == byte_options));
    }

    #[tokio::test]
    async fn scan_planning_supports_parallel_read_and_projection() {
        let source = PmcDatasetAccessor::new(MemoryAccessor::new(RAW.as_bytes(), 64));
        let request = PmcRequest::files(["metadata/PMC1.json", "metadata/PMC2.json"]).with_options(
            ReadOptions {
                projection: Some(vec!["pdf_url".into(), "pmcid".into()]),
                ..ReadOptions::default()
            },
        );
        let plan = source.plan_scan(request.clone()).await.unwrap();
        assert_eq!(
            plan.partitions.try_collect::<Vec<_>>().await.unwrap().len(),
            2
        );
        let mut reader = source
            .layer(ParallelReadLayer::new(
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::MIN,
            ))
            .open(request)
            .await
            .unwrap();
        let mut count = 0;
        while let Some(batch) = reader.next().await.unwrap() {
            assert_eq!(batch.schema().field(0).name(), "pdf_url");
            assert_eq!(batch.schema().field(1).name(), "pmcid");
            count += batch.num_rows();
        }
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn discovery_errors_propagate_and_end_the_scan() {
        let listing = Listing {
            fail: true,
            bytes: Some(MemoryAccessor::new(RAW.as_bytes(), 64)),
            ..Listing::default()
        };
        let mut reader = PmcDatasetAccessor::new(listing.clone())
            .open(PmcRequest::release())
            .await
            .unwrap();
        assert!(reader.next().await.unwrap().is_some());
        assert_eq!(
            reader.next().await.unwrap_err().kind(),
            ErrorKind::PermissionDenied
        );
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(listing.polled.load(Ordering::SeqCst), 2);
        let source = PmcDatasetAccessor::new(Listing {
            outside: true,
            ..Listing::default()
        });
        let mut paths = source.metadata_paths(&PmcRequest::release()).await.unwrap();
        assert_eq!(
            paths.try_next().await.unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        let source = PmcDatasetAccessor::new(MemoryAccessor::new(RAW.as_bytes(), 64));
        assert_eq!(
            source
                .open(PmcRequest::release())
                .await
                .err()
                .unwrap()
                .kind(),
            ErrorKind::Unsupported
        );
    }

    #[tokio::test]
    async fn validates_uris_paths_and_projection_before_listing() {
        for uri in [
            "pmc://oa",
            "pmc://oa/",
            "pmc://oa/metadata/",
            "pmc://oa/PMC123.1/",
            "pmc://oa/metadata/PMC123.1.json",
        ] {
            assert!(PmcRequest::from_uri(uri).is_ok(), "{uri}");
        }
        for uri in [
            "s3://pmc-oa-opendata/",
            "pmc://other/",
            "pmc://oa.evil/",
            "pmc://oa@host/",
            "pmc://oa:80/",
            "pmc://oa/metadata",
            "pmc://oa//",
            "pmc://oa/a//b/",
            "pmc://oa/a///",
            "pmc://oa/../",
            "pmc://oa/a/./",
            "pmc://oa/%2e%2e/",
            "pmc://oa/a\\b/",
            "pmc://oa/metadata/?limit=1",
            "pmc://oa/#fragment",
            "pmc://oa/ metadata/",
            "pmc://oa/metadata /",
            "pmc://oa/*/",
        ] {
            assert!(PmcRequest::from_uri(uri).is_err(), "{uri}");
        }
        let listing = Listing::default();
        let source = PmcDatasetAccessor::new(listing.clone());
        for request in [
            PmcRequest::prefix("metadata"),
            PmcRequest::files(["../a.json"]),
            PmcRequest::files(["a.pdf"]),
            PmcRequest::release().with_options(ReadOptions {
                projection: Some(vec!["unknown".into()]),
                ..ReadOptions::default()
            }),
        ] {
            assert_eq!(
                source.open(request).await.err().unwrap().kind(),
                ErrorKind::InvalidInput
            );
        }
        assert!(listing.requests.lock().unwrap().is_empty());
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
        PmcDatasetAccessor::public().unwrap();
    }
}

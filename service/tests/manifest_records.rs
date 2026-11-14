mod providers;

use std::num::NonZeroUsize;

use arrow_array::{Array, cast::AsArray, types::UInt64Type};
use arrow_schema::DataType;
use open_arrow_access_core::{
    Accessor, ErrorKind, OrderRequirement, ReadOptions, ReadRequest, Reader, ScanPlanner,
};
use open_arrow_access_service::{ManifestRecords, ResolvedFileSet, common::MetadataLink};
use open_arrow_access_storage::NamedFileAccessor;
use open_arrow_access_test_support::http::HttpFixture;

async fn rows(records: &ManifestRecords) -> Vec<(String, Option<u64>)> {
    let mut reader = records.open(ReadRequest::new("manifest")).await.unwrap();
    let mut output = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        let names = batch["file_name"].as_string::<i32>();
        let sizes = batch["size"].as_primitive::<UInt64Type>();
        output.extend((0..batch.num_rows()).map(|i| {
            (
                names.value(i).to_owned(),
                (!sizes.is_null(i)).then(|| sizes.value(i)),
            )
        }));
    }
    assert!(reader.next().await.unwrap().is_none());
    output
}

#[tokio::test]
async fn generic_constructor_needs_no_backend_traits_or_cloneable_metadata() {
    struct Metadata {
        revision: u32,
    }
    // () cannot open, list or stat storage. The manifest must be self-contained.
    let namespace = NamedFileAccessor::new(
        (),
        ["../opaque", " 雪 ", "a\0b", "a\nb"]
            .into_iter()
            .map(|name| (name.to_owned(), "private-download?token=secret".into()))
            .collect(),
    )
    .unwrap();
    let files = ResolvedFileSet::new(namespace, Metadata { revision: 4 });
    assert_eq!(files.metadata().revision, 4);
    let records = files.manifest_records();
    drop(files);
    let expected = vec![
        ("../opaque".into(), None),
        (" 雪 ".into(), None),
        ("a\0b".into(), None),
        ("a\nb".into(), None),
    ];
    assert_eq!(rows(&records).await, expected);
    assert_eq!(rows(&records.clone()).await, expected);
    let capability = records.capability();
    assert!(capability.read);
    assert!(!capability.read_with_range);
    assert!(!capability.read_parallel);
    assert!(!capability.list);
    assert_eq!(capability.ordering, OrderRequirement::PreserveGlobal);
}

#[tokio::test]
async fn aliases_stay_in_manifest_order_without_any_download_or_head_requests() {
    let server = HttpFixture::new();
    let connection = providers::connection(&server);
    let files = connection
        .files(
            ["z-last-name", "a-first-name", "alias"]
                .into_iter()
                .map(|name| MetadataLink {
                    file_name: name.into(),
                    url: format!("{}/same?signature=a%2Bb", server.endpoint()),
                })
                .collect(),
            ("provider-version", 42),
        )
        .unwrap();
    let records = files.manifest_records();
    assert_eq!(files.metadata(), &("provider-version", 42));
    assert_eq!(
        rows(&records).await,
        [
            ("z-last-name".into(), None),
            ("a-first-name".into(), None),
            ("alias".into(), None)
        ]
    );
    assert!(server.requests().is_empty());
    assert_eq!(
        records
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        ["file_name", "size"]
    );
    // Concurrently owned cursors start independently, including after the owner drops.
    let mut first = records
        .open(ReadRequest::new("first").with_options(ReadOptions {
            batch_size: NonZeroUsize::MIN,
            projection: None,
        }))
        .await
        .unwrap();
    let mut second = records.open(ReadRequest::new("second")).await.unwrap();
    assert_eq!(first.metadata().source.as_ref().unwrap().locator(), "first");
    assert_eq!(first.next().await.unwrap().unwrap().num_rows(), 1);
    drop(files);
    drop(records);
    assert_eq!(second.next().await.unwrap().unwrap().num_rows(), 3);
    assert_eq!(
        first.next().await.unwrap().unwrap()["file_name"]
            .as_string::<i32>()
            .value(0),
        "a-first-name"
    );
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn projection_batches_and_native_scan_plans_retain_counts_and_order() {
    let files = ResolvedFileSet::new(
        NamedFileAccessor::new(
            (),
            ["c", "a", "b"]
                .into_iter()
                .map(|name| (name.into(), "key".into()))
                .collect(),
        )
        .unwrap(),
        (),
    );
    let records = files.manifest_records();
    for projection in [
        vec!["size".to_owned(), "file_name".to_owned()],
        vec!["file_name".to_owned()],
        vec!["size".to_owned()],
        vec![],
    ] {
        let request = ReadRequest::new("projected").with_options(ReadOptions {
            batch_size: NonZeroUsize::new(2).unwrap(),
            projection: Some(projection.clone()),
        });
        let plan = records.plan_scan(request.clone()).await.unwrap();
        assert_eq!(plan.ordering, OrderRequirement::PreserveGlobal);
        let mut direct = records.open(request).await.unwrap();
        assert_eq!(direct.metadata().schema, plan.metadata.schema);
        let mut planned = plan.into_reader();
        let mut counts = Vec::new();
        while let Some(batch) = planned.next().await.unwrap() {
            assert_eq!(batch.num_columns(), projection.len());
            let expected = direct.next().await.unwrap().unwrap();
            assert_eq!(batch, expected);
            counts.push(batch.num_rows());
        }
        assert_eq!(counts, [2, 1]);
        assert!(direct.next().await.unwrap().is_none());
    }
    let mut reader = records
        .open(ReadRequest::new("large-batch").with_options(ReadOptions {
            batch_size: NonZeroUsize::new(usize::MAX).unwrap(),
            projection: None,
        }))
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 3);
}

#[tokio::test]
async fn empty_manifests_keep_the_schema_and_validate_projections() {
    let files = ResolvedFileSet::new(NamedFileAccessor::new((), vec![]).unwrap(), ());
    let records = files.manifest_records();
    assert_eq!(records.schema().field(0).data_type(), &DataType::Utf8);
    assert!(!records.schema().field(0).is_nullable());
    assert_eq!(records.schema().field(1).data_type(), &DataType::UInt64);
    assert!(records.schema().field(1).is_nullable());
    assert!(rows(&records).await.is_empty());
    let mut planned = records
        .plan_scan(ReadRequest::new("empty"))
        .await
        .unwrap()
        .into_reader();
    assert_eq!(planned.metadata().schema, records.schema());
    assert!(planned.next().await.unwrap().is_none());
    for projection in [
        vec!["unknown".to_owned()],
        vec!["file_name".to_owned(), "file_name".to_owned()],
    ] {
        let error = records
            .open(ReadRequest::new("invalid").with_options(ReadOptions {
                projection: Some(projection),
                ..Default::default()
            }))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
}

#[cfg(feature = "zenodo")]
#[tokio::test]
async fn zenodo_reported_sizes_include_zero_and_exact_u64_max_without_stat() {
    use open_arrow_access_service::zenodo::ZenodoService;
    let server = HttpFixture::new();
    server.respond("/api/records/1", http::StatusCode::OK,
        format!(r#"{{"id":1,"files":[{{"key":"big.bin","size":{},"checksum":"opaque"}},{{"key":"empty.bin","size":0,"checksum":"opaque"}}]}}"#, u64::MAX).into_bytes());
    let files = ZenodoService::with_endpoint(server.endpoint())
        .unwrap()
        .record(1.try_into().unwrap())
        .await
        .unwrap();
    let manifest = files.manifest_records();
    assert_eq!(
        rows(&manifest).await,
        [
            ("big.bin".into(), Some(u64::MAX)),
            ("empty.bin".into(), Some(0))
        ]
    );
    assert_eq!(files.metadata().files[0].size, u64::MAX);
    assert_eq!(server.requests().len(), 1);
    assert_eq!(server.requests()[0].path, "/api/records/1");
}

#[cfg(feature = "internet-archive")]
#[tokio::test]
async fn internet_archive_preserves_unknown_zero_and_known_sizes() {
    use open_arrow_access_service::internet_archive::InternetArchiveService;
    use open_arrow_access_test_support::fixture;
    let server = HttpFixture::new();
    server.respond(
        "/metadata/sample-item",
        http::StatusCode::OK,
        fixture!("internet_archive", "item.json").await,
    );
    let files = InternetArchiveService::with_endpoint(server.endpoint())
        .unwrap()
        .item("sample-item")
        .await
        .unwrap();
    assert_eq!(
        rows(&files.manifest_records()).await,
        [
            ("papers/paper #1%.pdf".into(), Some(8)),
            ("notes.xml".into(), None),
            ("empty.txt".into(), Some(0)),
        ]
    );
    assert_eq!(server.requests().len(), 1);
}

#[cfg(feature = "dataverse")]
#[tokio::test]
async fn dataverse_sizes_describe_original_downloads_and_logical_directory_names() {
    use open_arrow_access_service::dataverse::DataverseService;
    use open_arrow_access_test_support::fixture;
    let server = HttpFixture::new();
    let path = "/api/v1/datasets/:persistentId/versions/1.0?persistentId=doi%3A10.7910%2FDVN%2FA%26B%2B&excludeFiles=false&excludeMetadataBlocks=true";
    server.respond(
        path,
        http::StatusCode::OK,
        fixture!("dataverse", "dataset.json").await,
    );
    let files = DataverseService::new(server.endpoint())
        .unwrap()
        .dataset("doi:10.7910/DVN/A&B+", "1.0")
        .await
        .unwrap();
    assert_eq!(
        rows(&files.manifest_records()).await,
        [
            ("papers/2026/paper #1%.pdf".into(), Some(8)),
            ("table.tab".into(), Some(8)),
        ]
    );
    assert_eq!(files.metadata().version, "1.0");
    assert_eq!(server.requests().len(), 1);
}

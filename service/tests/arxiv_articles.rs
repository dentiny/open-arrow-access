use arrow_array::cast::AsArray;
use futures::StreamExt;
use open_arrow_access_core::{
    Accessor, ByteReadOptions, ByteReader, Bytes, ErrorKind, ObjectRef, ReadOptions, ScanPlanner,
    bytes_reader,
};
use open_arrow_access_service::arxiv::{
    ArxivArticle, ArxivArticleFormat as Format, ArxivArticleReadRequest, ArxivService,
    ArxivSourceOptions, ArxivSourcePackaging as Packaging, open_source_package,
};
use open_arrow_access_test_support::{MemoryAccessor, fixture};
use std::num::NonZeroUsize;

fn input(data: Vec<u8>) -> Box<dyn ByteReader> {
    Box::new(bytes_reader(
        Bytes::from(data),
        Some(ObjectRef::new("src/shard.tar#member=7")),
    ))
}
async fn load(name: &str) -> Vec<u8> {
    fixture!("arxiv-articles", name).await
}

#[test]
fn article_routes_accept_modern_legacy_and_versions_without_path_injection() {
    for id in [
        "2410.00001",
        "2410.00001v2",
        "hep-th/9901001v1",
        "solv-int/9901001",
    ] {
        for (format, prefix) in [
            (Format::Pdf, "pdf"),
            (Format::Source, "src"),
            (Format::Html, "html"),
        ] {
            assert_eq!(
                ArxivArticle::new(id, format).locator().unwrap(),
                format!("{prefix}/{id}")
            );
        }
    }
    for id in [
        "",
        "../2410.00001",
        "2410.00001?x=1",
        "2410.00001#f",
        "2410.00001v0",
        "2410.00001v01",
        "2410.00001v",
        "hep-th9901001",
        "2410.00001/extra",
    ] {
        assert!(
            ArxivArticle::new(id, Format::Html).locator().is_err(),
            "{id}"
        );
    }
}

#[tokio::test]
async fn explicit_formats_have_independent_lazy_partitions_and_preserve_payloads() {
    // Storage interprets routes; the reader never probes S3 manifests or other formats.
    let original = load("paper.html").await;
    let storage = MemoryAccessor::new(&original, 3);
    let observed = storage.observations.clone();
    let accessor = ArxivService::new(storage).article_records();
    let mut request = ArxivArticleReadRequest::new(vec![
        ArxivArticle::new("2410.00001v2", Format::Html),
        ArxivArticle::new("2410.00001v2", Format::Pdf),
        ArxivArticle::new("2410.00001v2", Format::Source),
    ]);
    request.byte_options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        ..Default::default()
    };
    let mut plan = accessor.plan_scan(request).await.unwrap();
    assert!(observed.requests.lock().unwrap().is_empty());
    let mut count = 0;
    while let Some(partition) = plan.partitions.next().await {
        let mut reader = partition.unwrap().await.unwrap();
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(
            batch
                .column_by_name("article_id")
                .unwrap()
                .as_string::<i64>()
                .value(0),
            "2410.00001v2"
        );
        assert_eq!(
            batch
                .column_by_name("data")
                .unwrap()
                .as_binary::<i64>()
                .value(0),
            original
        );
        assert!(reader.next().await.unwrap().is_none());
        count += 1;
    }
    assert_eq!(count, 3);
    let requests = observed.requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .map(|r| r.object.locator())
            .collect::<Vec<_>>(),
        vec!["html/2410.00001v2", "pdf/2410.00001v2", "src/2410.00001v2"]
    );
    assert!(requests.iter().all(|r| r.options.io_concurrency.get() == 3));
}

#[tokio::test]
async fn invalid_selection_and_projection_fail_before_any_storage_io() {
    let storage = MemoryAccessor::new(&load("paper.pdf").await, 2);
    let observed = storage.observations.clone();
    let accessor = ArxivService::new(storage).article_records();
    let request = ArxivArticleReadRequest::new(vec![
        ArxivArticle::new("2410.00001", Format::Pdf),
        ArxivArticle::new("../bad", Format::Html),
    ]);
    assert!(accessor.plan_scan(request).await.is_err());
    let mut request =
        ArxivArticleReadRequest::new(vec![ArxivArticle::new("2410.00001", Format::Pdf)]);
    request.options.projection = Some(vec!["missing".into()]);
    assert!(accessor.open(request).await.is_err());
    assert!(observed.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn article_size_limit_and_zero_column_projection() {
    let accessor =
        ArxivService::new(MemoryAccessor::new(&load("paper.pdf").await, 2)).article_records();
    let mut request =
        ArxivArticleReadRequest::new(vec![ArxivArticle::new("2410.00001", Format::Source)]);
    request.max_bytes = NonZeroUsize::new(3).unwrap();
    let mut reader = accessor.open(request).await.unwrap();
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
    assert!(reader.next().await.unwrap().is_none());
    let mut request =
        ArxivArticleReadRequest::new(vec![ArxivArticle::new("2410.00001", Format::Pdf)]);
    request.options.projection = Some(vec![]);
    let mut reader = accessor.open(request).await.unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!((batch.num_columns(), batch.num_rows()), (0, 1));
}

#[tokio::test]
async fn expands_tex_figures_video_and_data_preserving_duplicates_indices_and_bytes() {
    let mut members = Vec::new();
    for name in [
        "main.tex",
        "figures/a.png",
        "anc/demo.mp4",
        "anc/data.csv",
        "anc/metadata.xml",
        "anc/archive.zip",
    ] {
        members.push((name, load(&format!("members/{name}")).await));
    }
    members.push(("main.tex", load("duplicate.tex").await));
    let mut options = ArxivSourceOptions::default();
    options.options.batch_size = NonZeroUsize::new(2).unwrap();
    options.target_bytes = NonZeroUsize::new(1).unwrap();
    for packaging in [Packaging::Tar, Packaging::GzipTar] {
        let data = load(if matches!(packaging, Packaging::GzipTar) {
            "source.tar.gz"
        } else {
            "source.tar"
        })
        .await;
        let mut reader =
            open_source_package(input(data), "2410.00001v2", packaging, options.clone())
                .await
                .unwrap();
        let mut index = 0;
        while let Some(batch) = reader.next().await.unwrap() {
            assert_eq!(batch.num_rows(), 1); // soft byte target: whole members survive
            assert_eq!(
                batch
                    .column_by_name("entry_name")
                    .unwrap()
                    .as_binary::<i64>()
                    .value(0),
                members[index].0.as_bytes()
            );
            assert_eq!(
                batch
                    .column_by_name("entry_index")
                    .unwrap()
                    .as_primitive::<arrow_array::types::UInt64Type>()
                    .value(0),
                index as u64 + 1
            ); // directory skipped
            assert_eq!(
                batch
                    .column_by_name("data")
                    .unwrap()
                    .as_binary::<i64>()
                    .value(0),
                members[index].1
            );
            assert_eq!(
                batch
                    .column_by_name("format")
                    .unwrap()
                    .as_string::<i64>()
                    .value(0),
                "source_member"
            );
            assert_eq!(
                batch
                    .column_by_name("source_uri")
                    .unwrap()
                    .as_string::<i64>()
                    .value(0),
                "src/shard.tar#member=7"
            );
            index += 1;
        }
        assert_eq!(index, members.len());
    }
}

#[tokio::test]
async fn gzip_single_sources_and_empty_files_are_not_assumed_to_be_archives() {
    for name in ["main.tex", "paper.pdf", "empty.tex"] {
        let original = load(name).await;
        for compressed in [false, true] {
            let packaging = if compressed {
                Packaging::GzipSingle {
                    name: name.as_bytes().to_vec(),
                }
            } else {
                Packaging::Single {
                    name: name.as_bytes().to_vec(),
                }
            };
            let path = if compressed {
                format!("{name}.gz")
            } else {
                name.into()
            };
            let mut reader = open_source_package(
                input(load(&path).await),
                "hep-th/9901001v1",
                packaging,
                ArxivSourceOptions::default(),
            )
            .await
            .unwrap();
            let batch = reader.next().await.unwrap().unwrap();
            assert_eq!(batch["data"].as_binary::<i64>().value(0), original);
            assert_eq!(
                batch["entry_name"].as_binary::<i64>().value(0),
                name.as_bytes()
            );
            assert!(reader.next().await.unwrap().is_none());
        }
    }
}

#[tokio::test]
async fn rejects_gzip_corruption_truncation_trailing_bytes_and_decoded_bombs() {
    for name in ["bad-crc.tar.gz", "truncated.tar.gz", "trailing.tar.gz"] {
        assert!(
            open_source_package(
                input(load(name).await),
                "2410.00001",
                Packaging::GzipTar,
                ArxivSourceOptions::default()
            )
            .await
            .is_err()
        );
    }
    let valid = load("source.tar.gz").await;
    let options = ArxivSourceOptions {
        max_decoded_bytes: NonZeroUsize::new(10).unwrap(),
        ..Default::default()
    };
    assert!(
        open_source_package(input(valid), "2410.00001", Packaging::GzipTar, options)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn applies_tar_limits_and_preserves_non_utf8_names_without_filesystem_extraction() {
    let data = load("binary-name.tar").await;
    let mut reader = open_source_package(
        input(data.clone()),
        "2410.00001",
        Packaging::Tar,
        ArxivSourceOptions::default(),
    )
    .await
    .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(
        batch
            .column_by_name("entry_name")
            .unwrap()
            .as_binary::<i64>()
            .value(0),
        b"\xff.tex"
    );
    let mut options = ArxivSourceOptions::default();
    options.tar_limits.max_entry_bytes = 2;
    let mut reader = open_source_package(input(data), "2410.00001", Packaging::Tar, options)
        .await
        .unwrap();
    assert!(reader.next().await.is_err());
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn source_projection_retains_row_count_and_rejects_unknown_fields() {
    let data = load("source.tar").await;
    let mut options = ArxivSourceOptions {
        options: ReadOptions::default(),
        ..Default::default()
    };
    options.options.projection = Some(vec![]);
    let mut reader = open_source_package(
        input(data.clone()),
        "2410.00001",
        Packaging::Tar,
        options.clone(),
    )
    .await
    .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!((batch.num_columns(), batch.num_rows()), (0, 7));
    options.options.projection = Some(vec!["unknown".into()]);
    assert!(
        open_source_package(input(data), "2410.00001", Packaging::Tar, options)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn http_formats_preserve_distinct_bytes_and_missing_html_does_not_fallback() {
    use http::StatusCode;
    use open_arrow_access_storage::OpenDalAccessor;
    use open_arrow_access_test_support::http::HttpFixture;
    let http = HttpFixture::new();
    for (route, fixture) in [
        ("pdf", "paper.pdf"),
        ("src", "source.tar.gz"),
        ("html", "paper.html"),
    ] {
        http.file(
            &format!("/{route}/2410.00001v1"),
            Bytes::from(load(fixture).await),
        );
    }
    http.respond_for(
        http::Method::HEAD,
        "/html/2410.00002v1",
        StatusCode::NOT_FOUND,
        Bytes::new(),
    );
    http.respond("/html/2410.00002v1", StatusCode::NOT_FOUND, Bytes::new());
    let operator =
        opendal::Operator::new(opendal_service_http::Http::default().endpoint(http.endpoint()))
            .unwrap()
            .with_context(http.context());
    let accessor = ArxivService::new(OpenDalAccessor::new(operator)).article_records();
    let selections = vec![
        ArxivArticle::new("2410.00001v1", Format::Pdf),
        ArxivArticle::new("2410.00001v1", Format::Source),
        ArxivArticle::new("2410.00001v1", Format::Html),
    ];
    let mut reader = accessor
        .open(ArxivArticleReadRequest::new(selections))
        .await
        .unwrap();
    for name in ["paper.pdf", "source.tar.gz", "paper.html"] {
        let expected = load(name).await;
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(
            batch
                .column_by_name("data")
                .unwrap()
                .as_binary::<i64>()
                .value(0),
            expected
        );
    }
    assert!(reader.next().await.unwrap().is_none());
    http.clear_requests();
    let mut reader = accessor
        .open(ArxivArticleReadRequest::new(vec![ArxivArticle::new(
            "2410.00002v1",
            Format::Html,
        )]))
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::NotFound);
    assert!(
        http.requests()
            .iter()
            .all(|r| r.path == "/html/2410.00002v1")
    );
}

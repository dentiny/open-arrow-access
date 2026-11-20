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
use open_arrow_access_test_support::MemoryAccessor;
use std::{io::Write, num::NonZeroUsize};

fn input(data: Vec<u8>) -> Box<dyn ByteReader> {
    Box::new(bytes_reader(
        Bytes::from(data),
        Some(ObjectRef::new("src/shard.tar#member=7")),
    ))
}
fn tar(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let mut tar = tar::Builder::new(Vec::new());
    let mut dir = tar::Header::new_ustar();
    dir.set_entry_type(tar::EntryType::Directory);
    dir.set_size(0);
    dir.set_mode(0o755);
    dir.set_cksum();
    tar.append_data(&mut dir, "figures/", &b""[..]).unwrap();
    for (name, data) in entries {
        let mut header = tar::Header::new_ustar();
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar.append_data(&mut header, name, *data).unwrap();
    }
    tar.into_inner().unwrap()
}
fn gzip(data: &[u8]) -> Vec<u8> {
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gzip.write_all(data).unwrap();
    gzip.finish().unwrap()
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
    let storage = MemoryAccessor::new(b"<html>full text<img src='x.png'></html>", 3);
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
            b"<html>full text<img src='x.png'></html>"
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
    let storage = MemoryAccessor::new(b"payload", 2);
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
    let accessor = ArxivService::new(MemoryAccessor::new(b"payload", 2)).article_records();
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
    let members: Vec<(&str, &[u8])> = vec![
        ("main.tex", br"\includegraphics{figures/a.png}"),
        ("figures/a.png", b"\x89PNG\0\xff"),
        ("anc/demo.mp4", b"\0video\xff"),
        ("anc/data.csv", b"x,y\n1,2"),
        ("main.tex", b"duplicate"),
    ];
    let mut options = ArxivSourceOptions::default();
    options.options.batch_size = NonZeroUsize::new(2).unwrap();
    options.target_bytes = NonZeroUsize::new(1).unwrap();
    for packaging in [Packaging::Tar, Packaging::GzipTar] {
        let data = tar(&members);
        let data = if matches!(packaging, Packaging::GzipTar) {
            gzip(&data)
        } else {
            data
        };
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
    for data in [
        b"\\documentclass{article}".as_slice(),
        b"%PDF-fallback",
        b"",
    ] {
        for packaging in [
            Packaging::Single {
                name: b"original".to_vec(),
            },
            Packaging::GzipSingle {
                name: b"original".to_vec(),
            },
        ] {
            let compressed = matches!(packaging, Packaging::GzipSingle { .. });
            let mut reader = open_source_package(
                input(if compressed {
                    gzip(data)
                } else {
                    data.to_vec()
                }),
                "hep-th/9901001v1",
                packaging,
                ArxivSourceOptions::default(),
            )
            .await
            .unwrap();
            let batch = reader.next().await.unwrap().unwrap();
            assert_eq!(
                batch
                    .column_by_name("data")
                    .unwrap()
                    .as_binary::<i64>()
                    .value(0),
                data
            );
            assert!(reader.next().await.unwrap().is_none());
        }
    }
}

#[tokio::test]
async fn rejects_gzip_corruption_truncation_trailing_bytes_and_decoded_bombs() {
    let bytes = tar(&[("main.tex", b"hello")]);
    let valid = gzip(&bytes);
    let mut corrupt = valid.clone();
    let index = corrupt.len() - 8;
    corrupt[index] ^= 1;
    let mut trailing = valid.clone();
    trailing.extend_from_slice(b"garbage");
    for invalid in [corrupt, valid[..valid.len() - 4].to_vec(), trailing] {
        assert!(
            open_source_package(
                input(invalid),
                "2410.00001",
                Packaging::GzipTar,
                ArxivSourceOptions::default()
            )
            .await
            .is_err()
        );
    }
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
    let mut data = tar(&[("file", b"abc")]);
    // First regular header follows a directory. Change name bytes and re-checksum.
    let header = &mut data[512..1024];
    header[..5].copy_from_slice(b"\xff.tex");
    header[148..156].fill(b' ');
    let checksum: u32 = header.iter().map(|b| *b as u32).sum();
    header[148..156].copy_from_slice(format!("{checksum:06o}\0 ").as_bytes());
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
    let data = tar(&[("a.tex", b"a"), ("anc/b.mp4", b"b")]);
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
    assert_eq!((batch.num_columns(), batch.num_rows()), (0, 2));
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
    http.file("/pdf/2410.00001v1", Bytes::from_static(b"%PDF-original"));
    http.file("/src/2410.00001v1", Bytes::from_static(b"source-original"));
    http.file(
        "/html/2410.00001v1",
        Bytes::from_static(b"<html>original</html>"),
    );
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
    for expected in [
        b"%PDF-original".as_slice(),
        b"source-original",
        b"<html>original</html>",
    ] {
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

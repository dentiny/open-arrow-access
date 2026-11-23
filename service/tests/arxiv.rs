use open_arrow_access_decompression::tar::TarEntryReader;
use std::num::NonZeroUsize;

use arrow_array::cast::AsArray;
use open_arrow_access_core::{Accessor, AccessorExt, ByteReadOptions, ErrorKind, ScanPlanner};
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_service::arxiv::{ArxivKind, ArxivReadRequest, ArxivService};
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::{
    MemoryAccessor, byte_layer, expect_err_kind, fixture, fixture_path, fs_operator, temp_fs,
};

fn service() -> ArxivService<OpenDalAccessor> {
    ArxivService::new(OpenDalAccessor::new(fs_operator(fixture_path!(
        "arxiv", ""
    ))))
}

#[tokio::test]
async fn resolves_manifest_order_and_reads_selected_pdf_without_other_bodies() {
    let service = service();
    let archives = service
        .manifest(ArxivKind::Pdf, Some(2410), ByteReadOptions::default())
        .await
        .unwrap();
    assert_eq!(archives.len(), 2);
    assert_eq!(
        archives.iter().map(|a| a.sequence).collect::<Vec<_>>(),
        vec![3, 1]
    );
    let archive = service
        .archive(&archives[1], ByteReadOptions::default())
        .await
        .unwrap();
    assert_eq!(archive.entries().len(), 4);
    let mut reader = archive
        .open_entry(1, NonZeroUsize::new(4).unwrap())
        .unwrap();
    drop(archive);
    drop(service);
    byte_layer::assert_output(reader.as_mut(), b"%PDF-first-paper", 4)
        .await
        .unwrap();
}

#[tokio::test]
async fn supports_source_manifests_and_empty_month_selection() {
    let service = service();
    // The source fixture omits first/last item metadata; PDF fixtures retain it.
    let source = service
        .manifest(ArxivKind::Source, None, ByteReadOptions::default())
        .await
        .unwrap();
    assert_eq!(source[0].filename, "src/arXiv_src_2410_001.tar");
    assert!(
        service
            .manifest(ArxivKind::Pdf, Some(2501), ByteReadOptions::default())
            .await
            .unwrap()
            .is_empty()
    );
    let all = service
        .manifest(ArxivKind::Pdf, None, ByteReadOptions::default())
        .await
        .unwrap();
    assert_eq!(all.len(), 3);
}

#[tokio::test]
async fn forwards_byte_options_and_validates_before_opening_storage() {
    let bytes = fixture!("arxiv", "pdf/arXiv_pdf_manifest.xml").await;
    let source = MemoryAccessor::new(&bytes, 3);
    let observed = source.observations.clone();
    let service = ArxivService::new(source);
    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        ..Default::default()
    };
    for month in [2413, 2400, 10000] {
        expect_err_kind(
            service.manifest(ArxivKind::Pdf, Some(month), options).await,
            ErrorKind::InvalidInput,
        );
    }
    assert!(observed.requests.lock().unwrap().is_empty());
    let selected = service
        .manifest(ArxivKind::Pdf, Some(2410), options)
        .await
        .unwrap();
    let request = observed.requests.lock().unwrap()[0].clone();
    assert_eq!(request.object.locator(), "pdf/arXiv_pdf_manifest.xml");
    assert_eq!(request.options, options);
    let bytes = fixture!("arxiv", "pdf/arXiv_pdf_2410_001.tar").await;
    let source = MemoryAccessor::new(&bytes, 3).with_ranges();
    let observed = source.observations.clone();
    let service = ArxivService::new(source);
    service.archive(&selected[1], options).await.unwrap();
    assert_eq!(observed.requests.lock().unwrap()[0].options, options);
}

#[tokio::test]
async fn rejects_invalid_manifests_and_changed_archive_length() {
    for name in [
        "duplicate.xml",
        "traversal.xml",
        "bad-month.xml",
        "wrong-root.xml",
        "malformed.xml",
        "trailing-text.xml",
        "second-root.xml",
        "trailing-malformed.xml",
        "trailing-reference.xml",
        "trailing-unknown-reference.xml",
        "duplicate-declaration.xml",
    ] {
        let bytes = fixture!("arxiv", name).await;
        let source = MemoryAccessor::new(&bytes, 3);
        assert_eq!(
            ArxivService::new(source)
                .manifest(ArxivKind::Pdf, None, ByteReadOptions::default())
                .await
                .err()
                .unwrap()
                .kind(),
            ErrorKind::Decode,
            "{name}"
        );
    }
    let empty = fixture!("arxiv", "empty.xml").await;
    assert!(
        ArxivService::new(MemoryAccessor::new(&empty, 3))
            .manifest(ArxivKind::Pdf, None, ByteReadOptions::default())
            .await
            .unwrap()
            .is_empty()
    );
    let archives = service()
        .manifest(ArxivKind::Pdf, Some(2410), ByteReadOptions::default())
        .await
        .unwrap();
    let changed = ArxivService::new(MemoryAccessor::new(b"short", 3).with_ranges());
    expect_err_kind(
        changed
            .archive(&archives[1], ByteReadOptions::default())
            .await,
        ErrorKind::Decode,
    );
    expect_err_kind(
        changed
            .stream(&archives[1], ByteReadOptions::default())
            .await,
        ErrorKind::Decode,
    );
}

#[tokio::test]
async fn bulk_stream_starts_before_other_headers_and_accepts_sequential_storage() {
    use std::sync::atomic::Ordering;

    let archives = service()
        .manifest(ArxivKind::Pdf, Some(2410), ByteReadOptions::default())
        .await
        .unwrap();
    let bytes = fixture!("arxiv", "pdf/arXiv_pdf_2410_001.tar").await;
    let source = MemoryAccessor::new(&bytes, 64).without_content_length();
    let observed = source.observations.clone();
    let service = ArxivService::new(source);
    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        ..Default::default()
    };
    let mut stream = service.stream(&archives[1], options).await.unwrap();
    drop(stream.next_entry().await.unwrap().unwrap());
    let TarEntryReader { entry, mut reader } = stream.next_entry().await.unwrap().unwrap();
    assert_eq!(entry.index, 1);
    assert_eq!(observed.reads.load(Ordering::SeqCst), 16);
    assert_eq!(observed.range_reads.load(Ordering::SeqCst), 0);
    assert_eq!(observed.requests.lock().unwrap()[0].options, options);
    drop(service);
    drop(stream);
    byte_layer::assert_output(reader.as_mut(), b"%PDF-first-paper", 64)
        .await
        .unwrap();
}

#[tokio::test]
async fn numeric_month_preserves_zero_padded_archive_names() {
    let raw = fixture!("arxiv", "pdf/arXiv_pdf_manifest.xml").await;
    let raw = String::from_utf8(raw).unwrap().replace("2410", "0001");
    let service = ArxivService::new(MemoryAccessor::new(raw.as_bytes(), 64));
    let archives = service
        .manifest(ArxivKind::Pdf, Some(1), ByteReadOptions::default())
        .await
        .unwrap();
    assert_eq!(archives.len(), 2);
    assert_eq!(archives[0].month, 1);
    assert_eq!(archives[0].filename, "pdf/arXiv_pdf_0001_003.tar");
}

async fn native_record_fixture() -> (tempfile::TempDir, opendal::Operator) {
    let manifest = fixture!("arxiv", "pdf/arXiv_pdf_manifest.xml").await;
    let archive = fixture!("arxiv", "pdf/arXiv_pdf_2410_001.tar").await;
    temp_fs(&[
        ("pdf/arXiv_pdf_manifest.xml", &manifest),
        ("pdf/arXiv_pdf_2410_003.tar", &archive),
        ("pdf/arXiv_pdf_2410_001.tar", &archive),
    ])
}

#[tokio::test]
async fn native_parallel_shards_preserve_order_and_payloads_with_a_soft_byte_target() {
    let (_dir, operator) = native_record_fixture().await;
    let records = ArxivService::new(OpenDalAccessor::new(operator)).records();
    // A payload may exceed the target, but it must remain intact in its own batch.
    let request = ArxivReadRequest::new(ArxivKind::Pdf)
        .with_month(2410)
        .with_target_bytes(NonZeroUsize::MIN);
    let plan = records.plan_scan(request.clone()).await.unwrap();
    assert_eq!(plan.partitions.size_hint().0, 2);
    let mut expected = records.open(request.clone()).await.unwrap();
    let mut actual = records
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::MIN,
        ))
        .open(request)
        .await
        .unwrap();
    for payload in [b"%PDF-first-paper".as_slice(), b"%PDF-second-paper", b""].repeat(2) {
        let batch = actual.next().await.unwrap().unwrap();
        assert_eq!(batch, expected.next().await.unwrap().unwrap());
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch["data"].as_binary::<i64>().value(0), payload);
    }
    assert!(actual.next().await.unwrap().is_none());
    assert!(expected.next().await.unwrap().is_none());
}

#[tokio::test]
async fn auto_discovers_both_distributions_and_default_scan() {
    let service = service();
    let archives = service
        .manifest(ArxivKind::Auto, Some(2410), ByteReadOptions::default())
        .await
        .unwrap();
    assert_eq!(
        archives
            .iter()
            .map(|a| (a.kind, a.sequence))
            .collect::<Vec<_>>(),
        vec![
            (ArxivKind::Pdf, 3),
            (ArxivKind::Pdf, 1),
            (ArxivKind::Source, 1)
        ]
    );
    let plan = service
        .records()
        .plan_scan(ArxivReadRequest::default().with_month(2410))
        .await
        .unwrap();
    assert_eq!(plan.partitions.size_hint().0, 3);
}

#[tokio::test]
async fn auto_skips_missing_distributions_but_requires_one_manifest() {
    for available in [ArxivKind::Pdf, ArxivKind::Source] {
        let dir = tempfile::tempdir().unwrap();
        let key = available.manifest_key().unwrap();
        let destination = dir.path().join(key);
        std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
        std::fs::copy(fixture_path!("arxiv", key), &destination).unwrap();
        let service = ArxivService::new(OpenDalAccessor::new(fs_operator(dir.path())));
        let archives = service
            .manifest(ArxivKind::Auto, None, ByteReadOptions::default())
            .await
            .unwrap();
        assert!(!archives.is_empty());
        assert!(archives.iter().all(|archive| archive.kind == available));
        std::fs::remove_file(destination).unwrap();
        assert_eq!(
            service
                .manifest(ArxivKind::Auto, None, ByteReadOptions::default())
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::NotFound
        );
    }
}

#[tokio::test]
async fn auto_propagates_storage_and_manifest_errors() {
    let pdf = fixture!("arxiv", "pdf/arXiv_pdf_manifest.xml").await;
    for (storage, expected) in [
        (
            MemoryAccessor::new(b"", 3).with_open_failure(ErrorKind::PermissionDenied),
            ErrorKind::PermissionDenied,
        ),
        // PDF succeeds, but returning the same XML for source has the wrong root.
        (MemoryAccessor::new(&pdf, 3), ErrorKind::Decode),
    ] {
        assert_eq!(
            ArxivService::new(storage)
                .manifest(ArxivKind::Auto, None, ByteReadOptions::default())
                .await
                .unwrap_err()
                .kind(),
            expected
        );
    }
}

#[tokio::test]
async fn manifest_payload_sizes_allow_tar_framing_in_stream_index_and_native_scan() {
    let (dir, operator) = native_record_fixture().await;
    let manifest = dir.path().join("pdf/arXiv_pdf_manifest.xml");
    // Upstream size is the sum of member payloads, not the padded TAR length.
    let payload_size = b"%PDF-first-paper".len() + b"%PDF-second-paper".len();
    let xml = std::fs::read_to_string(&manifest).unwrap().replace(
        "<size>10240</size>",
        &format!("<size>{payload_size}</size>"),
    );
    std::fs::write(manifest, xml).unwrap();
    let service = ArxivService::new(OpenDalAccessor::new(operator));
    let shards = service
        .manifest(ArxivKind::Pdf, Some(2410), ByteReadOptions::default())
        .await
        .unwrap();
    let archive = service
        .archive(&shards[0], ByteReadOptions::default())
        .await
        .unwrap();
    assert_eq!(
        archive
            .entries()
            .filter(|e| e.is_file)
            .map(|e| e.size)
            .sum::<u64>(),
        payload_size as u64
    );
    let mut reader = service
        .records()
        .open(ArxivReadRequest::new(ArxivKind::Pdf).with_month(2410))
        .await
        .unwrap();
    let mut payloads = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        payloads.extend(
            batch["data"]
                .as_binary::<i64>()
                .iter()
                .map(|v| v.unwrap().to_vec()),
        );
    }
    assert_eq!(
        payloads,
        [
            b"%PDF-first-paper".to_vec(),
            b"%PDF-second-paper".to_vec(),
            Vec::new()
        ]
        .into_iter()
        .cycle()
        .take(6)
        .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn explicit_shard_validates_selection_and_reads_only_that_object() {
    use futures::TryStreamExt;
    let bytes = fixture!("arxiv", "pdf/arXiv_pdf_2410_001.tar").await;
    let source = MemoryAccessor::new(&bytes, 512);
    let observed = source.observations.clone();
    let records = ArxivService::new(source).records();
    for shard in [
        "../other.tar",
        "/absolute.tar",
        "s3://bucket/shard.tar",
        "part.tar?x=1",
        "part.tar.gz",
        "",
    ] {
        assert!(
            records
                .plan_scan(ArxivReadRequest::new(ArxivKind::Source).with_shard(shard))
                .await
                .is_err()
        );
    }
    assert!(
        records
            .plan_scan(
                ArxivReadRequest::new(ArxivKind::Source)
                    .with_shard("src/one.tar")
                    .with_month(2410)
            )
            .await
            .is_err()
    );
    assert!(observed.requests.lock().unwrap().is_empty());
    let shard = "pdf/arXiv_pdf_2410_001.tar";
    let plan = records
        .plan_scan(ArxivReadRequest::new(ArxivKind::Pdf).with_shard(shard))
        .await
        .unwrap();
    let mut partitions = plan.partitions.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(partitions.len(), 1);
    assert!(observed.requests.lock().unwrap().is_empty());
    let mut reader = partitions.pop().unwrap().await.unwrap();
    while let Some(batch) = reader.next().await.unwrap() {
        assert!(
            batch["shard_uri"]
                .as_string::<i64>()
                .iter()
                .all(|value| value == Some(shard))
        );
    }
    let requests = observed.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].object.locator(), shard);
}

#[tokio::test]
async fn explicit_raw_shard_streams_with_one_get_and_no_head() {
    use open_arrow_access_test_support::http::HttpFixture;
    let http = HttpFixture::new();
    let shard = "src/one.tar";
    http.respond(
        "/src/one.tar",
        http::StatusCode::OK,
        fixture!("arxiv-content", "latex-two.tar").await,
    );
    let operator =
        opendal::Operator::new(opendal_service_http::Http::default().endpoint(http.endpoint()))
            .unwrap()
            .with_context(http.context());
    let records = ArxivService::new(OpenDalAccessor::new(operator)).records();
    let mut reader = records
        .open(ArxivReadRequest::new(ArxivKind::Source).with_shard(shard))
        .await
        .unwrap();
    assert!(http.requests().is_empty());
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        rows += batch.num_rows();
        assert!(
            batch["shard_uri"]
                .as_string::<i64>()
                .iter()
                .all(|value| value == Some(shard))
        );
    }
    assert_eq!(rows, 2);
    let requests = http.requests();
    assert_eq!(requests.len(), 1, "{requests:?}");
    assert_eq!(requests[0].method, http::Method::GET);
    assert_eq!(requests[0].path, "/src/one.tar");
}

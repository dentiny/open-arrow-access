use std::num::NonZeroUsize;

use arrow_array::cast::AsArray;
use open_arrow_access_core::{Accessor, AccessorExt, ByteReadOptions, ErrorKind, ScanPlanner};
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_service::arxiv::{ArxivKind, ArxivReadRequest, ArxivService};
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::{
    MemoryAccessor, byte_layer, fixture, fixture_path, fs_operator,
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
        assert_eq!(
            service
                .manifest(ArxivKind::Pdf, Some(month), options)
                .await
                .err()
                .unwrap()
                .kind(),
            ErrorKind::InvalidInput
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
    assert_eq!(
        changed
            .archive(&archives[1], ByteReadOptions::default())
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Decode
    );
    assert_eq!(
        changed
            .stream(&archives[1], ByteReadOptions::default())
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Decode
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
    let (entry, mut reader) = stream.next_entry().await.unwrap().unwrap();
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

fn native_record_fixture() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("pdf")).unwrap();
    std::fs::copy(
        fixture_path!("arxiv", "pdf/arXiv_pdf_manifest.xml"),
        dir.path().join("pdf/arXiv_pdf_manifest.xml"),
    )
    .unwrap();
    for name in ["arXiv_pdf_2410_003.tar", "arXiv_pdf_2410_001.tar"] {
        std::fs::copy(
            fixture_path!("arxiv", "pdf/arXiv_pdf_2410_001.tar"),
            dir.path().join("pdf").join(name),
        )
        .unwrap();
    }
    dir
}

#[tokio::test]
async fn native_parallel_shards_preserve_order_and_payloads_with_a_soft_byte_target() {
    let dir = native_record_fixture();
    let records = ArxivService::new(OpenDalAccessor::new(fs_operator(dir.path()))).records();
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

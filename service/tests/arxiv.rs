use std::num::NonZeroUsize;

use open_arrow_access_core::{ByteReadOptions, ErrorKind};
use open_arrow_access_service::arxiv::{ArxivKind, ArxivService};
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::{
    MemoryAccessor, collect_bytes, fixture, fixture_path, fs_operator,
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
        .manifest(ArxivKind::Pdf, Some("2410"), ByteReadOptions::default())
        .await
        .unwrap();
    assert_eq!(archives.len(), 2);
    assert_eq!(
        archives.iter().map(|a| a.sequence).collect::<Vec<_>>(),
        vec![3, 1]
    );
    assert_eq!(archives[1].first_item, "2410.00001");
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
    assert_eq!(
        collect_bytes(reader.as_mut()).await.unwrap(),
        b"%PDF-first-paper"
    );
}

#[tokio::test]
async fn supports_source_manifests_and_empty_month_selection() {
    let service = service();
    let source = service
        .manifest(ArxivKind::Source, None, ByteReadOptions::default())
        .await
        .unwrap();
    assert_eq!(source[0].filename, "src/arXiv_src_2410_001.tar");
    assert!(
        service
            .manifest(ArxivKind::Pdf, Some("2501"), ByteReadOptions::default())
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
    for month in ["2413", "2400", "202410", "24é"] {
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
        .manifest(ArxivKind::Pdf, Some("2410"), options)
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
async fn rejects_invalid_bounded_manifests_and_changed_archive_length() {
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
    let bytes = fixture!("arxiv", "pdf/arXiv_pdf_manifest.xml").await;
    assert_eq!(
        ArxivService::new(MemoryAccessor::new(&bytes, 3))
            .with_manifest_limit(8)
            .manifest(ArxivKind::Pdf, None, ByteReadOptions::default())
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Decode
    );
    let empty = fixture!("arxiv", "empty.xml").await;
    assert!(
        ArxivService::new(MemoryAccessor::new(&empty, 3))
            .manifest(ArxivKind::Pdf, None, ByteReadOptions::default())
            .await
            .unwrap()
            .is_empty()
    );
    let archives = service()
        .manifest(ArxivKind::Pdf, Some("2410"), ByteReadOptions::default())
        .await
        .unwrap();
    assert_eq!(
        ArxivService::new(MemoryAccessor::new(b"short", 3).with_ranges())
            .archive(&archives[1], ByteReadOptions::default())
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
        .manifest(ArxivKind::Pdf, Some("2410"), ByteReadOptions::default())
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
    assert_eq!(
        collect_bytes(reader.as_mut()).await.unwrap(),
        b"%PDF-first-paper"
    );
}

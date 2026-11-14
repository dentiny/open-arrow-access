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

// Route known keys without listing, like a partial bulk mirror.
struct ManifestStorage {
    pdf: MemoryAccessor,
    source: MemoryAccessor,
}

#[open_arrow_access_core::async_trait]
impl Accessor for ManifestStorage {
    type Request = open_arrow_access_core::ByteReadRequest;
    type Reader = dyn open_arrow_access_core::ByteReader;

    fn capability(&self) -> open_arrow_access_core::Capability {
        self.pdf.capability()
    }

    async fn open(
        &self,
        request: Self::Request,
    ) -> open_arrow_access_core::Result<Box<Self::Reader>> {
        match request.object.locator() {
            "pdf/arXiv_pdf_manifest.xml" => self.pdf.open(request).await,
            "src/arXiv_src_manifest.xml" => self.source.open(request).await,
            key => panic!("unexpected storage request: {key}"),
        }
    }
}

#[tokio::test]
async fn auto_discovers_both_kinds_in_order_and_forwards_options_without_listing() {
    let pdf = MemoryAccessor::new(&fixture!("arxiv", "pdf/arXiv_pdf_manifest.xml").await, 3);
    let source = MemoryAccessor::new(&fixture!("arxiv", "src/arXiv_src_manifest.xml").await, 3);
    let observations = [pdf.observations.clone(), source.observations.clone()];
    let service = ArxivService::new(ManifestStorage { pdf, source });
    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        ..Default::default()
    };
    let archives = service
        .manifest(ArxivKind::Auto, Some(2410), options)
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
    for observed in observations {
        let requests = observed.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].options, options);
    }
    assert_eq!(
        service
            .manifest(ArxivKind::Auto, None, options)
            .await
            .unwrap()
            .len(),
        4
    );
    assert!(
        service
            .manifest(ArxivKind::Auto, Some(2501), options)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn auto_supports_either_missing_distribution_but_requires_one_manifest() {
    for available in [ArxivKind::Pdf, ArxivKind::Source] {
        let dir = tempfile::tempdir().unwrap();
        let key = available.manifest_key().unwrap();
        let destination = dir.path().join(key);
        std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
        std::fs::copy(fixture_path!("arxiv", key), destination).unwrap();
        let service = ArxivService::new(OpenDalAccessor::new(fs_operator(dir.path())));
        let archives = service
            .manifest(ArxivKind::Auto, None, ByteReadOptions::default())
            .await
            .unwrap();
        assert!(!archives.is_empty());
        assert!(archives.iter().all(|archive| archive.kind == available));
        let missing = match available {
            ArxivKind::Pdf => ArxivKind::Source,
            ArxivKind::Source => ArxivKind::Pdf,
            ArxivKind::Auto => unreachable!(),
        };
        assert_eq!(
            service
                .manifest(missing, None, ByteReadOptions::default())
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::NotFound
        );
        std::fs::remove_file(dir.path().join(key)).unwrap();
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
async fn auto_does_not_hide_storage_or_decode_errors_in_either_manifest() {
    for kind in [
        ErrorKind::PermissionDenied,
        ErrorKind::Unauthenticated,
        ErrorKind::Io,
        ErrorKind::Timeout,
    ] {
        for fail_pdf in [true, false] {
            let pdf =
                MemoryAccessor::new(&fixture!("arxiv", "pdf/arXiv_pdf_manifest.xml").await, 3);
            let source =
                MemoryAccessor::new(&fixture!("arxiv", "src/arXiv_src_manifest.xml").await, 3);
            let storage = if fail_pdf {
                ManifestStorage {
                    pdf: pdf.with_open_failure(kind),
                    source,
                }
            } else {
                ManifestStorage {
                    pdf,
                    source: source.with_open_failure(kind),
                }
            };
            assert_eq!(
                ArxivService::new(storage)
                    .manifest(ArxivKind::Auto, None, ByteReadOptions::default())
                    .await
                    .unwrap_err()
                    .kind(),
                kind
            );
        }
    }
    for fail_pdf in [true, false] {
        let pdf = MemoryAccessor::new(&fixture!("arxiv", "pdf/arXiv_pdf_manifest.xml").await, 3);
        let source = MemoryAccessor::new(&fixture!("arxiv", "src/arXiv_src_manifest.xml").await, 3);
        let malformed = MemoryAccessor::new(b"<wrong-root/>", 3);
        let storage = if fail_pdf {
            ManifestStorage {
                pdf: malformed,
                source,
            }
        } else {
            ManifestStorage {
                pdf,
                source: malformed,
            }
        };
        assert_eq!(
            ArxivService::new(storage)
                .manifest(ArxivKind::Auto, None, ByteReadOptions::default())
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Decode
        );
    }
}

#[tokio::test]
async fn auto_accepts_an_empty_manifest_and_validates_month_before_io() {
    let pdf = MemoryAccessor::new(&fixture!("arxiv", "empty.xml").await, 3);
    let source = MemoryAccessor::new(b"", 3).with_open_failure(ErrorKind::NotFound);
    let observed = pdf.observations.clone();
    let service = ArxivService::new(ManifestStorage { pdf, source });
    assert_eq!(
        service
            .manifest(ArxivKind::Auto, Some(2413), ByteReadOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert!(observed.requests.lock().unwrap().is_empty());
    assert!(
        service
            .manifest(ArxivKind::Auto, None, ByteReadOptions::default())
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn auto_is_a_selection_policy_and_cannot_be_used_as_a_resolved_archive_kind() {
    assert_eq!(ArxivKind::default(), ArxivKind::Auto);
    assert_eq!(ArxivKind::Auto.manifest_key(), None);
    let mut archive = service()
        .manifest(ArxivKind::Pdf, Some(2410), ByteReadOptions::default())
        .await
        .unwrap()
        .remove(0);
    archive.kind = ArxivKind::Auto;
    let storage = MemoryAccessor::new(b"", 3);
    let observed = storage.observations.clone();
    let service = ArxivService::new(storage);
    assert_eq!(
        service
            .stream(&archive, ByteReadOptions::default())
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Decode
    );
    assert!(observed.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn default_record_request_discovers_and_reads_both_distributions() {
    let dir = native_record_fixture();
    std::fs::create_dir_all(dir.path().join("src")).unwrap();
    std::fs::copy(
        fixture_path!("arxiv", "src/arXiv_src_manifest.xml"),
        dir.path().join("src/arXiv_src_manifest.xml"),
    )
    .unwrap();
    // The record API preserves entry bytes for either distribution.
    std::fs::copy(
        fixture_path!("arxiv", "pdf/arXiv_pdf_2410_001.tar"),
        dir.path().join("src/arXiv_src_2410_001.tar"),
    )
    .unwrap();
    let records = ArxivService::new(OpenDalAccessor::new(fs_operator(dir.path()))).records();
    let request = ArxivReadRequest::default().with_month(2410);
    assert_eq!(request.kind, ArxivKind::Auto);
    let plan = records.plan_scan(request.clone()).await.unwrap();
    assert_eq!(plan.partitions.size_hint().0, 3);
    let mut reader = records.open(request).await.unwrap();
    for expected in [
        "pdf/arXiv_pdf_2410_003.tar",
        "pdf/arXiv_pdf_2410_001.tar",
        "src/arXiv_src_2410_001.tar",
    ] {
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch["shard_uri"].as_string::<i64>().value(0), expected);
        assert_eq!(
            batch["data"].as_binary::<i64>().value(0),
            b"%PDF-first-paper"
        );
    }
    assert!(reader.next().await.unwrap().is_none());
}

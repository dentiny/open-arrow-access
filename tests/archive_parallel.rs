use std::{num::NonZeroUsize, sync::atomic::Ordering};

use arrow_array::types::UInt64Type;
use futures::TryStreamExt;
use open_arrow_access::{
    Accessor, AccessorExt, ArchiveAccessor, ArchiveFormat, ArchiveOptions, EmissionPolicy,
    ReadRequest, ScanPlanner,
    crawl::{CrawlAccessor, CrawlOptions},
    dataset::file_set::FileSetRequest,
    layers::ParallelReadLayer,
};
use open_arrow_access_test_support::{MemoryAccessor, fixture, record::collect_column};

#[tokio::test]
async fn parallel_read_opens_archive_once_and_preserves_entries() {
    let bytes = fixture!("tar", "mixed.tar").await;
    let source = MemoryAccessor::new(&bytes, 512);
    let observed = source.observations.clone();
    let archive = ArchiveAccessor::new(source, ArchiveOptions::new(ArchiveFormat::Tar)).layer(
        ParallelReadLayer::new(NonZeroUsize::new(2).unwrap(), NonZeroUsize::MIN),
    );
    let reader = archive
        .open(ReadRequest::new("archive.tar").with_batch_size(NonZeroUsize::MIN))
        .await
        .unwrap();
    assert!(observed.requests.lock().unwrap().is_empty());
    assert_eq!(collect_column::<UInt64Type>(reader, 1).await, [1, 2, 3]);
    assert_eq!(observed.requests.lock().unwrap().len(), 1);
    assert_eq!(observed.range_reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn partitions_preserve_row_and_byte_batch_targets_and_projection() {
    let bytes = fixture!("tar", "mixed.tar").await;
    for (rows, target, expected) in [
        (10, 1, 2),
        (10, 4 * 1024 * 1024, 1),
        (1, 4 * 1024 * 1024, 2),
    ] {
        let mut options = ArchiveOptions::new(ArchiveFormat::Tar)
            .with_entry_filter(|entry| Ok(entry.name.ends_with(b".pdf")));
        options.target_bytes = NonZeroUsize::new(target).unwrap();
        let archive = ArchiveAccessor::new(MemoryAccessor::new(&bytes, 64), options);
        let plan = archive
            .plan_scan(
                ReadRequest::new("archive.tar")
                    .with_batch_size(NonZeroUsize::new(rows).unwrap())
                    .with_projection(["entry_index"]),
            )
            .await
            .unwrap();
        assert!(plan.streaming_discovery);
        let partitions = plan.partitions.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(partitions.len(), expected);
        let mut indices = Vec::new();
        for partition in partitions {
            indices.extend(collect_column::<UInt64Type>(partition.await.unwrap(), 0).await);
        }
        assert_eq!(indices, [1, 2]);
    }
}

#[tokio::test]
async fn multiple_archives_keep_file_order_without_losing_batches() {
    let bytes = fixture!("tar", "mixed.tar").await;
    let source = MemoryAccessor::new(&bytes, 512);
    let observed = source.observations.clone();
    let archive = ArchiveAccessor::new(source, ArchiveOptions::new(ArchiveFormat::Tar));
    let crawl = CrawlAccessor::files(
        archive,
        CrawlOptions::default().with_emission(EmissionPolicy::Ordered),
    );
    let reader = crawl
        .open(FileSetRequest::new(["first.tar", "second.tar"]))
        .await
        .unwrap();
    assert_eq!(
        collect_column::<UInt64Type>(reader, 1).await,
        [1, 2, 3, 1, 2, 3]
    );
    assert_eq!(observed.requests.lock().unwrap().len(), 2);
}

use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow_array::types::UInt64Type;
use futures::{StreamExt, TryStreamExt};
use open_arrow_access::{
    Accessor, AccessorExt, ArchiveAccessor, ArchiveFormat, ArchiveOptions, EmissionPolicy,
    ReadRequest, RecordCapability, RecordReader, Result, ScanPartition, ScanPlan, ScanPlanner,
    async_trait,
    crawl::{CrawlAccessor, CrawlOptions},
    dataset::file_set::FileSetRequest,
    layers::ParallelReadLayer,
};
use open_arrow_access_test_support::{MemoryAccessor, fixture, record::collect_column};
use tokio::sync::{Notify, Semaphore};

// Model downstream record processing: hold the first batch while the second
// runs, without adding a scheduler or touching the shared TAR reader.
struct Gate {
    archive: ArchiveAccessor,
    calls: Arc<AtomicUsize>,
    started: Arc<Semaphore>,
    release: Arc<Notify>,
}
#[async_trait]
impl Accessor for Gate {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    fn capability(&self) -> RecordCapability {
        self.archive.capability()
    }
    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        Ok(Box::new(self.plan_scan(request).await?.into_reader()))
    }
}
#[async_trait]
impl ScanPlanner for Gate {
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        let mut plan = self.archive.plan_scan(request).await?;
        let (calls, started, release) = (
            self.calls.clone(),
            self.started.clone(),
            self.release.clone(),
        );
        plan.partitions = plan
            .partitions
            .map_ok(move |partition| {
                let (calls, started, release) = (calls.clone(), started.clone(), release.clone());
                Box::pin(async move {
                    let reader = partition.await?;
                    let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
                    started.add_permits(1);
                    if first {
                        release.notified().await;
                    }
                    Ok(reader)
                }) as ScanPartition
            })
            .boxed();
        Ok(plan)
    }
}

#[tokio::test]
async fn batches_process_in_parallel_with_one_archive_open_and_bounded_discovery() {
    let bytes = fixture!("tar", "mixed.tar").await;
    let source = MemoryAccessor::new(&bytes, 512);
    let observed = source.observations.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Semaphore::new(0));
    let release = Arc::new(Notify::new());
    let archive = Gate {
        archive: ArchiveAccessor::new(source, ArchiveOptions::new(ArchiveFormat::Tar)),
        calls: calls.clone(),
        started: started.clone(),
        release: release.clone(),
    }
    .layer(ParallelReadLayer::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::MIN,
    ));
    let mut reader = archive
        .open(ReadRequest::new("archive.tar").with_batch_size(NonZeroUsize::MIN))
        .await
        .unwrap();
    assert!(observed.requests.lock().unwrap().is_empty());
    let first = tokio::spawn(async move {
        let batch = reader.next().await.unwrap().unwrap();
        (batch, reader)
    });
    started.acquire_many(2).await.unwrap().forget();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(
        !first.is_finished(),
        "ordered output waits for the first batch"
    );
    assert_eq!(observed.requests.lock().unwrap().len(), 1);
    release.notify_one();
    let (batch, reader) = first.await.unwrap();
    assert_eq!(batch.num_rows(), 1);
    let remaining = collect_column::<UInt64Type>(reader, 1).await;
    assert_eq!(remaining, [2, 3]);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
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

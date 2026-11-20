use super::*;
use crate::FileSetRequest;
use futures::TryStreamExt;
use open_arrow_access_core::{Accessor, AccessorExt, Error, ErrorKind, ReadOptions, ScanPlanner};
use open_arrow_access_layers::{ParallelReadLayer, UnorderedEmissionLayer};
use open_arrow_access_test_support::MemoryAccessor;
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

const KEYS: [&str; 6] = [
    "ocr/item-1/item-1-1-1.txt",
    "ocr/item-1/item-1-2-2.txt",
    "ocr/item-1/item-1-3-3.txt",
    "ocr/item-1/item-1-4-4.txt",
    "ocr/item-1/item-1-5-5.txt",
    "ocr/item-1/item-1-6-6.txt",
];

#[tokio::test]
async fn planning_and_opening_partitions_never_call_the_provider() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let pages = BhlPageAccessor::new(MemoryAccessor::new(b"OCR", 3), "snapshot", move |_| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(super::super::page::tests::metadata("Title", "Rights"))
    })
    .unwrap();
    let plan = pages.plan_scan(FileSetRequest::new(KEYS)).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let mut readers = Vec::new();
    for partition in plan.partitions.try_collect::<Vec<_>>().await.unwrap() {
        readers.push(partition.await.unwrap());
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(readers[0].next().await.unwrap().is_some());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(readers[0].next().await.unwrap().is_none());
    drop(readers);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let reader = pages.open(FileSetRequest::new(KEYS)).await.unwrap();
    drop(reader);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn parallel_resident_budget_and_backpressure_bound_metadata_preparation() {
    for partitions in [1, 2, 3] {
        for unordered in [false, true] {
            let calls = Arc::new(AtomicUsize::new(0));
            let count = calls.clone();
            let started = Arc::new(Semaphore::new(0));
            let signal = started.clone();
            let pages =
                BhlPageAccessor::new(MemoryAccessor::new(b"OCR", 3), "snapshot", move |_| {
                    count.fetch_add(1, Ordering::SeqCst);
                    signal.add_permits(1);
                    Ok(super::super::page::tests::metadata("Title", "Rights"))
                })
                .unwrap()
                .layer(ParallelReadLayer::new(
                    NonZeroUsize::new(partitions).unwrap(),
                    NonZeroUsize::MIN,
                ));
            let request = FileSetRequest::new(KEYS)
                .with_options(ReadOptions::default().with_projection(["metadata", "page_id"]));
            let mut reader = if unordered {
                pages
                    .layer(UnorderedEmissionLayer)
                    .open(request)
                    .await
                    .unwrap()
            } else {
                pages.open(request).await.unwrap()
            };
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
            tokio::time::timeout(
                Duration::from_secs(5),
                started.acquire_many(u32::try_from(partitions).unwrap()),
            )
            .await
            .unwrap()
            .unwrap()
            .forget();
            // The consumer has paused. Even completed partition queues keep their
            // resident slots, so the remaining pages must not prepare metadata.
            tokio::task::yield_now().await;
            assert_eq!(calls.load(Ordering::SeqCst), partitions);
            drop(reader);
            tokio::task::yield_now().await;
            assert_eq!(calls.load(Ordering::SeqCst), partitions);
        }
    }
}

#[tokio::test]
async fn provider_failures_are_lazy_preserved_and_stop_later_partitions() {
    let storage = MemoryAccessor::new(b"OCR", 3);
    let observed = storage.observations.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let pages = BhlPageAccessor::new(storage, "snapshot", move |_| {
        count.fetch_add(1, Ordering::SeqCst);
        Err(Error::new(
            ErrorKind::PermissionDenied,
            "metadata-provider",
            "source denied",
        ))
    })
    .unwrap();
    let mut reader = pages.open(FileSetRequest::new(KEYS)).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert!(error.to_string().contains("metadata-provider"));
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(observed.requests.lock().unwrap().is_empty());
}

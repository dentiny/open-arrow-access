use std::num::NonZeroUsize;

use arrow_array::{cast::AsArray, types::Int64Type};
use futures::TryStreamExt;
use http::StatusCode;
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteReadOptions, Bytes, ErrorKind, ReadOptions, ScanPlanner,
};
use open_arrow_access_test_support::fixture;

use super::super::HuggingFaceDatasetRequest;
use super::support::{COMMIT, ExportHub};
use open_arrow_access_layers::ParallelReadLayer;

fn request() -> HuggingFaceDatasetRequest {
    HuggingFaceDatasetRequest::new("owner/example", "default", "train")
}

#[tokio::test]
async fn dataset_pins_export_sorts_paginated_shards_and_reads_lazily_with_options() {
    let hub = ExportHub::new("paginated.json").await;
    let service = hub.service().await;
    let mut reader = service
        .open(
            request()
                .with_options(ReadOptions {
                    batch_size: NonZeroUsize::MIN,
                    projection: Some(vec!["id".into()]),
                })
                .with_byte_options(ByteReadOptions {
                    io_concurrency: NonZeroUsize::new(2).unwrap(),
                    io_chunk_size: NonZeroUsize::new(64),
                }),
        )
        .await
        .unwrap();
    assert!(!hub.requested("0001.parquet"));
    assert_eq!(
        reader.capability().ordering,
        open_arrow_access_core::OrderRequirement::PreserveGlobal
    );
    let mut ids = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.num_rows(), 1);
        ids.push(batch.column(0).as_primitive::<Int64Type>().value(0));
    }
    assert_eq!(ids, [1, 2, 3]);
    let requests = hub.requests();
    assert_eq!(
        requests
            .iter()
            .filter(|uri| uri.contains("/revision/"))
            .count(),
        1
    );
    assert!(requests.iter().any(|uri| uri.contains("cursor=second")));
    assert!(
        !requests
            .iter()
            .any(|uri| uri.contains("/resolve/")
                && (uri.contains("README") || uri.contains("/test/")))
    );
    assert!(hub.ranges().iter().all(|range| range.len() <= 64));
}

#[tokio::test]
async fn commit_requests_skip_revision_metadata() {
    let hub = ExportHub::new("train.json").await;
    assert_eq!(hub.read_ids(request().with_revision(COMMIT)).await, [1, 2]);
    assert!(!hub.requested("/revision/"));
}

#[tokio::test]
async fn partial_exports_require_opt_in_and_do_not_hide_permission_failures() {
    let mut hub = ExportHub::new("partial.json").await;
    let service = hub.service().await;
    let error = service.dataset(request()).await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::NotFound);
    assert!(!hub.requested("partial-train"));
    assert_eq!(hub.read_ids(request().with_partial_exports()).await, [1, 2]);
    hub.full_status = StatusCode::FORBIDDEN;
    hub.clear_requests();
    let error = hub
        .service()
        .await
        .dataset(request().with_partial_exports())
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert!(!hub.requested("partial-train"));
}

#[tokio::test]
async fn complete_exports_take_precedence_over_partial_exports() {
    let hub = ExportHub::new("complete.json").await;
    assert_eq!(hub.read_ids(request().with_partial_exports()).await, [1, 2]);
    assert!(!hub.requested("partial-train"));
}

#[tokio::test]
async fn invalid_dataset_selection_fails_before_network_io() {
    let hub = ExportHub::new("empty.json").await;
    let service = hub.service().await;
    for selection in [
        HuggingFaceDatasetRequest::new("../repo", "default", "train"),
        HuggingFaceDatasetRequest::new("owner/example", "default", "partial-train"),
    ] {
        let error = service.dataset(selection).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
    assert!(hub.requests().is_empty());
}

#[tokio::test]
async fn invalid_revision_metadata_never_opens_files() {
    let mut hub = ExportHub::new("empty.json").await;
    for (name, expected) in [
        ("bad_truncated_revision.json", ErrorKind::Decode),
        ("bad_revision_sha.json", ErrorKind::Decode),
        ("bad_oversized_revision.json", ErrorKind::Decode),
    ] {
        hub.metadata = Bytes::from(fixture!("huggingface", name).await);
        hub.clear_requests();
        let error = hub.service().await.dataset(request()).await.err().unwrap();
        assert_eq!(error.kind(), expected);
        assert_eq!(hub.requests().len(), 1);
    }
}

#[tokio::test]
async fn revision_metadata_can_exceed_the_previous_size_limit() {
    let mut hub = ExportHub::new("complete.json").await;
    let mut metadata = vec![b' '; 64 * 1024 + 1];
    metadata.extend_from_slice(&hub.metadata);
    hub.metadata = metadata.into();
    assert_eq!(hub.read_ids(request()).await, [1, 2]);
}

#[tokio::test]
async fn parallel_dataset_preserves_pinned_shard_order_and_partial_admission() {
    let hub = ExportHub::new("paginated.json").await;
    let service = hub.service().await;
    let plan = service.plan_scan(request()).await.unwrap();
    assert_eq!(
        service.capability().ordering,
        open_arrow_access_core::OrderRequirement::PreserveGlobal
    );
    assert_eq!(plan.ordering, service.capability().ordering);
    assert!(!hub.requested("0001.parquet"));
    let partitions: Vec<_> = plan.partitions.try_collect().await.unwrap();
    assert_eq!(partitions.len(), 2);
    drop(partitions);
    hub.clear_requests();
    let mut reader = service
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ))
        .open(request().with_options(ReadOptions {
            batch_size: NonZeroUsize::MIN,
            projection: Some(vec!["id".into()]),
        }))
        .await
        .unwrap();
    assert_eq!(
        reader.capability().ordering,
        open_arrow_access_core::OrderRequirement::PreserveGlobal
    );
    let mut ids = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        ids.push(batch.column(0).as_primitive::<Int64Type>().value(0));
    }
    assert_eq!(ids, [1, 2, 3]);
    assert_eq!(
        hub.requests()
            .iter()
            .filter(|uri| uri.contains("/revision/"))
            .count(),
        1
    );
    let hub = ExportHub::new("partial.json").await;
    let service = hub.service().await;
    assert_eq!(
        service.plan_scan(request()).await.err().unwrap().kind(),
        ErrorKind::NotFound
    );
    service
        .plan_scan(request().with_partial_exports())
        .await
        .unwrap();
}

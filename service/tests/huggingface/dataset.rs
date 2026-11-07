use std::num::NonZeroUsize;

use arrow_array::{cast::AsArray, types::Int64Type};
use http::StatusCode;
use open_arrow_access_core::{Accessor, ByteReadOptions, Bytes, ErrorKind, ReadOptions};

use super::super::HuggingFaceDatasetRequest;
use super::support::{COMMIT, ExportEntry, ExportHub};

const TRAIN_A: ExportEntry<'_> = ExportEntry {
    repository_path: "default/train/0000.parquet",
    fixture_path: "directory/a.parquet",
};

fn request() -> HuggingFaceDatasetRequest {
    HuggingFaceDatasetRequest::new("owner/example", "default", "train")
}

#[tokio::test]
async fn dataset_pins_export_sorts_paginated_shards_and_reads_lazily_with_options() {
    let hub = ExportHub::new(&[
        TRAIN_A,
        ExportEntry {
            repository_path: "default/train/0001.parquet",
            fixture_path: "directory/b.parquet",
        },
        ExportEntry {
            repository_path: "default/train/README.md",
            fixture_path: "ids.parquet",
        },
        ExportEntry {
            repository_path: "default/test/0000.parquet",
            fixture_path: "directory/hidden.parquet",
        },
    ])
    .await;
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
    assert!(!reader.capability().read_parallel);
    assert!(!reader.capability().read_with_range);
    assert!(!reader.capability().list);
    assert!(!hub.requested("0001.parquet"));
    let mut ids = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.schema(), reader.metadata().schema);
        ids.push(batch.column(0).as_primitive::<Int64Type>().value(0));
    }
    assert_eq!(ids, [1, 2, 3]);
    assert_eq!(reader.metadata().schema.metadata()["label"], "same");
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
    let count = requests.len();
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(hub.requests().len(), count);
}

#[tokio::test]
async fn dataset_preserves_zero_column_row_counts_and_skips_metadata_for_commit_requests() {
    let hub = ExportHub::new(&[TRAIN_A]).await;
    let mut reader = hub
        .service()
        .await
        .dataset(request().with_revision(COMMIT).with_options(ReadOptions {
            projection: Some(vec![]),
            ..ReadOptions::default()
        }))
        .await
        .unwrap();
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.num_columns(), 0);
        rows += batch.num_rows();
    }
    assert_eq!(rows, 2);
    assert!(!hub.requested("/revision/"));
}

#[tokio::test]
async fn partial_exports_require_opt_in_and_do_not_hide_permission_failures() {
    let mut hub = ExportHub::new(&[ExportEntry {
        repository_path: "default/partial-train/0000.parquet",
        fixture_path: "directory/a.parquet",
    }])
    .await;
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
    let hub = ExportHub::new(&[
        TRAIN_A,
        ExportEntry {
            repository_path: "default/partial-train/0000.parquet",
            fixture_path: "directory/hidden.parquet",
        },
    ])
    .await;
    assert_eq!(hub.read_ids(request().with_partial_exports()).await, [1, 2]);
    assert!(!hub.requested("partial-train"));
}

#[tokio::test]
async fn invalid_dataset_selection_fails_before_network_io() {
    let hub = ExportHub::new(&[]).await;
    let service = hub.service().await;
    for selection in [
        HuggingFaceDatasetRequest::new("../repo", "default", "train"),
        HuggingFaceDatasetRequest::new("owner/example", "", "train"),
        HuggingFaceDatasetRequest::new("owner/example", "default/other", "train"),
        HuggingFaceDatasetRequest::new("owner/example", "default", "../test"),
        HuggingFaceDatasetRequest::new("owner/example", "default", "partial-train"),
    ] {
        let error = service.dataset(selection).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
    assert!(hub.requests().is_empty());
}

#[tokio::test]
async fn invalid_revision_metadata_never_opens_files() {
    let mut hub = ExportHub::new(&[]).await;
    for metadata in [
        b"{truncated".to_vec(),
        b"{}".to_vec(),
        b"{\"sha\":\"main\"}".to_vec(),
        vec![b' '; 65537],
    ] {
        hub.metadata = Bytes::from(metadata);
        hub.clear_requests();
        let error = hub.service().await.dataset(request()).await.err().unwrap();
        assert!(matches!(
            error.kind(),
            ErrorKind::Decode | ErrorKind::Unsupported
        ));
        assert_eq!(hub.requests().len(), 1);
    }
}

#[tokio::test]
async fn dataset_reuses_file_set_schema_validation_and_terminates_on_mismatch() {
    let hub = ExportHub::new(&[
        TRAIN_A,
        ExportEntry {
            repository_path: "default/train/0001.parquet",
            fixture_path: "ids.parquet",
        },
    ])
    .await;
    let mut reader = hub.service().await.dataset(request()).await.unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 2);
    let error = reader.next().await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    let count = hub.requests().len();
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(hub.requests().len(), count);
}

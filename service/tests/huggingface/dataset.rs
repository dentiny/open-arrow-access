use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use arrow_array::Int64Array;
use http::{Request, Response, StatusCode};
use open_arrow_access_core::{
    Accessor, ByteReadOptions, Bytes, ErrorKind, ReadOptions, RecordReader,
};
use opendal::{Buffer, HttpBody, HttpTransport};
use serde_json::json;

use super::super::{HuggingFaceDatasetRequest, HuggingFaceService};
use super::support::{MockHub, TOKEN, response, service_with};

const COMMIT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

struct ExportEntry<'a> {
    repository_path: &'a str,
    fixture_path: &'a str,
}

#[derive(Clone)]
struct ExportHub {
    files: MockHub,
    paths: Vec<(String, usize)>,
    metadata: Bytes,
    full_status: StatusCode,
    requests: Arc<Mutex<Vec<String>>>,
}

impl ExportHub {
    async fn new(entries: &[ExportEntry<'_>]) -> Self {
        let mut files = Vec::new();
        for entry in entries {
            let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../testdata/parquet")
                .join(entry.fixture_path);
            files.push((
                entry.repository_path.to_owned(),
                Bytes::from(tokio::fs::read(fixture).await.unwrap()),
            ));
        }
        Self {
            paths: files
                .iter()
                .map(|(path, bytes)| (path.clone(), bytes.len()))
                .collect(),
            files: MockHub::with_files(files),
            metadata: Bytes::from(json!({"sha": COMMIT}).to_string()),
            full_status: StatusCode::OK,
            requests: Arc::default(),
        }
    }

    async fn service(&self) -> HuggingFaceService {
        service_with(self.clone()).await
    }

    fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}

impl HttpTransport for ExportHub {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        assert_eq!(
            request.headers()["authorization"],
            format!("Bearer {TOKEN}")
        );
        let uri = request.uri().to_string();
        self.requests.lock().unwrap().push(uri.clone());
        if uri.contains("/revision/") {
            assert!(uri.contains("refs%2Fconvert%2Fparquet"));
            assert!(uri.ends_with("expand=sha"));
            return Ok(response(StatusCode::OK, self.metadata.clone()));
        }
        assert!(
            uri.contains(COMMIT),
            "all membership and file I/O must use the pinned commit"
        );
        if let Some((_, rest)) = uri.split_once("/tree/") {
            assert!(!uri.contains("recursive=True"));
            let prefix = rest.split_once('/').unwrap().1.split('?').next().unwrap();
            if prefix == "default/train" && self.full_status != StatusCode::OK {
                return Ok(response(self.full_status, Bytes::new()));
            }
            let prefix = format!("{prefix}/");
            // Return reverse order and paginate to exercise provider ordering.
            let files: Vec<_> = self.paths.iter().rev().filter(|(path, _)| path.starts_with(&prefix))
                .map(|(path, size)| json!({"type":"file", "path":path, "size":size, "oid":"fixture"})).collect();
            if files.is_empty() {
                return Ok(response(StatusCode::NOT_FOUND, Bytes::new()));
            }
            let page2 = uri.contains("cursor=second");
            let page = if page2 { &files[1..] } else { &files[..1] };
            let mut output = response(
                StatusCode::OK,
                Bytes::from(serde_json::to_vec(page).unwrap()),
            );
            if !page2 && files.len() > 1 {
                output.headers_mut().insert(
                    "link",
                    format!("<{uri}&cursor=second>; rel=\"next\"")
                        .parse()
                        .unwrap(),
                );
            }
            return Ok(output);
        }
        self.files.fetch(request).await
    }
}

fn request() -> HuggingFaceDatasetRequest {
    HuggingFaceDatasetRequest::new("owner/example", "default", "train")
}

async fn collect_ids(mut reader: Box<dyn RecordReader>) -> Vec<i64> {
    let mut ids = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        ids.extend(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied(),
        );
    }
    ids
}

#[tokio::test]
async fn dataset_pins_export_sorts_paginated_shards_and_reads_lazily_with_options() {
    let hub = ExportHub::new(&[
        ExportEntry {
            repository_path: "default/train/0000.parquet",
            fixture_path: "directory/a.parquet",
        },
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
    assert!(
        !hub.requests()
            .iter()
            .any(|uri| uri.contains("/resolve/") && uri.contains("0001.parquet"))
    );
    let mut ids = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.schema(), reader.metadata().schema);
        ids.push(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
        );
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
    let ranges = hub.files.captured();
    assert!(
        ranges
            .iter()
            .filter_map(|request| request.range.as_ref())
            .all(|range| {
                let (start, end) = range
                    .strip_prefix("bytes=")
                    .unwrap()
                    .split_once('-')
                    .unwrap();
                end.parse::<u64>().unwrap() - start.parse::<u64>().unwrap() < 64
            })
    );
    let count = requests.len();
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(hub.requests().len(), count);
}

#[tokio::test]
async fn dataset_preserves_zero_column_row_counts_and_skips_metadata_for_commit_requests() {
    let hub = ExportHub::new(&[ExportEntry {
        repository_path: "default/train/0000.parquet",
        fixture_path: "directory/a.parquet",
    }])
    .await;
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
    assert!(!hub.requests().iter().any(|uri| uri.contains("/revision/")));
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
    assert!(
        !hub.requests()
            .iter()
            .any(|uri| uri.contains("partial-train"))
    );
    assert_eq!(
        collect_ids(
            service
                .dataset(request().with_partial_exports())
                .await
                .unwrap()
        )
        .await,
        [1, 2]
    );
    hub.full_status = StatusCode::FORBIDDEN;
    hub.requests.lock().unwrap().clear();
    let error = hub
        .service()
        .await
        .dataset(request().with_partial_exports())
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert!(
        !hub.requests()
            .iter()
            .any(|uri| uri.contains("partial-train"))
    );
}

#[tokio::test]
async fn complete_exports_take_precedence_over_partial_exports() {
    let hub = ExportHub::new(&[
        ExportEntry {
            repository_path: "default/train/0000.parquet",
            fixture_path: "directory/a.parquet",
        },
        ExportEntry {
            repository_path: "default/partial-train/0000.parquet",
            fixture_path: "directory/hidden.parquet",
        },
    ])
    .await;
    assert_eq!(
        collect_ids(
            hub.service()
                .await
                .dataset(request().with_partial_exports())
                .await
                .unwrap()
        )
        .await,
        [1, 2]
    );
    assert!(
        !hub.requests()
            .iter()
            .any(|uri| uri.contains("partial-train"))
    );
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
        hub.requests.lock().unwrap().clear();
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
        ExportEntry {
            repository_path: "default/train/0000.parquet",
            fixture_path: "directory/a.parquet",
        },
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

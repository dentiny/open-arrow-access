use std::{collections::BTreeMap, ops::Range, sync::Arc};

use arrow_array::types::Int64Type;
use http::StatusCode;
use open_arrow_access_core::Bytes;
use open_arrow_access_test_support::{
    fixture,
    http::{HttpFixture, file_response, response},
    record::collect_column,
};
use serde::Deserialize;
use serde_json::Value;

use super::super::{HuggingFaceDatasetRequest, HuggingFaceOptions, HuggingFaceService};

pub(super) const COMMIT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const TOKEN: &str = "fixture-only-token";

#[derive(Deserialize)]
struct ExportEntry {
    fixture: String,
    metadata: Value,
}

pub(super) struct ExportHub {
    files: Arc<BTreeMap<String, Bytes>>,
    pub(super) metadata: Bytes,
    catalog: Arc<Vec<Value>>,
    pub(super) full_status: StatusCode,
    http: HttpFixture,
}

impl ExportHub {
    pub(super) async fn new(name: &str) -> Self {
        let entries: Vec<ExportEntry> =
            serde_json::from_slice(&fixture!("huggingface/exports", name).await).unwrap();
        let mut files = BTreeMap::new();
        let mut catalog = Vec::new();
        for entry in entries {
            let bytes = fixture!("parquet", &entry.fixture).await;
            assert_eq!(entry.metadata["size"].as_u64().unwrap(), bytes.len() as u64);
            files.insert(
                entry.metadata["path"].as_str().unwrap().to_owned(),
                Bytes::from(bytes),
            );
            catalog.push(entry.metadata);
        }
        Self {
            files: Arc::new(files),
            metadata: Bytes::from(fixture!("huggingface", "revision.json").await),
            catalog: Arc::new(catalog),
            full_status: StatusCode::OK,
            http: HttpFixture::new(),
        }
    }

    pub(super) async fn service(&self) -> HuggingFaceService {
        let files = self.files.clone();
        let metadata = self.metadata.clone();
        let catalog = self.catalog.clone();
        let full_status = self.full_status;
        self.http.handle(move |request| {
            assert_eq!(
                request.headers()["authorization"],
                format!("Bearer {TOKEN}")
            );
            let uri = request.uri().to_string();
            if uri.contains("/revision/") {
                assert!(uri.contains("refs%2Fconvert%2Fparquet"));
                assert!(uri.ends_with("expand=sha"));
                return Ok(response(StatusCode::OK, metadata.clone()));
            }
            assert!(
                uri.contains(COMMIT),
                "membership and file I/O must use the pinned commit"
            );
            if let Some((_, rest)) = uri.split_once("/tree/") {
                assert!(!uri.contains("recursive=True"));
                let prefix = rest.split_once('/').unwrap().1.split('?').next().unwrap();
                if prefix == "default/train" && full_status != StatusCode::OK {
                    return Ok(response(full_status, Bytes::new()));
                }
                // Reverse path order and paginate to exercise provider ordering.
                let prefix = format!("{prefix}/");
                let mut files: Vec<_> = catalog
                    .iter()
                    .filter(|entry| {
                        let path = entry["path"].as_str().unwrap();
                        path.starts_with(&prefix) && files.contains_key(path)
                    })
                    .collect();
                files.sort_by(|a, b| b["path"].as_str().cmp(&a["path"].as_str()));
                if files.is_empty() {
                    return Ok(response(StatusCode::NOT_FOUND, Bytes::new()));
                }
                let page = if uri.contains("cursor=second") {
                    &files[1..]
                } else {
                    &files[..1]
                };
                let mut output = response(
                    StatusCode::OK,
                    Bytes::from(serde_json::to_vec(page).unwrap()),
                );
                if !uri.contains("cursor=second") && files.len() > 1 {
                    output.headers_mut().insert(
                        "link",
                        format!("<{uri}&cursor=second>; rel=\"next\"")
                            .parse()
                            .unwrap(),
                    );
                }
                return Ok(output);
            }
            if uri.contains("/paths-info/") {
                let form = String::from_utf8(request.body().to_bytes().to_vec()).unwrap();
                let path = form
                    .strip_prefix("paths=")
                    .unwrap()
                    .split('&')
                    .next()
                    .unwrap();
                let files: Vec<_> = catalog
                    .iter()
                    .filter(|entry| entry["path"] == path && files.contains_key(path))
                    .collect();
                return Ok(response(
                    StatusCode::OK,
                    Bytes::from(serde_json::to_vec(&files).unwrap()),
                ));
            }
            if let Some((_, path)) = uri
                .split_once("/resolve/")
                .and_then(|(_, rest)| rest.split_once('/'))
            {
                let bytes = files.get(path).unwrap();
                return Ok(file_response(&request, bytes.clone()));
            }
            panic!("unexpected request: {uri}")
        });
        HuggingFaceService::new(
            HuggingFaceOptions::default()
                .with_endpoint(self.http.endpoint())
                .with_token(TOKEN),
        )
        .await
        .unwrap()
    }

    pub(super) async fn read_ids(&self, request: HuggingFaceDatasetRequest) -> Vec<i64> {
        let reader = self.service().await.dataset(request).await.unwrap();
        collect_column::<Int64Type>(reader, 0).await
    }

    pub(super) fn requests(&self) -> Vec<String> {
        self.http
            .requests()
            .into_iter()
            .map(|request| request.path)
            .collect()
    }

    pub(super) fn requested(&self, fragment: &str) -> bool {
        self.requests().iter().any(|uri| uri.contains(fragment))
    }

    pub(super) fn clear_requests(&self) {
        self.http.clear_requests();
    }

    pub(super) fn ranges(&self) -> Vec<Range<usize>> {
        self.http
            .requests()
            .iter()
            .filter_map(|request| request.range())
            .collect()
    }
}

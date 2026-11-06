use std::sync::{Arc, Mutex};

use futures::stream;
use http::{Request, Response, StatusCode};
use open_arrow_access_core::{Accessor, Bytes, ErrorKind, ObjectRef, ReadRequest, Result};
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter, OperationContext};
use serde_json::{Value, json};

use super::support::{HTTP_ERRORS, MockHub, TOKEN, assert_upstream_error, response};
use crate::huggingface::{
    HuggingFaceDataset, HuggingFaceOptions, HuggingFaceSelection, HuggingFaceService,
};

const COMMIT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[derive(Clone)]
struct MetadataHub {
    metadata: Bytes,
    status: StatusCode,
    files: MockHub,
    requests: Arc<Mutex<Vec<String>>>,
}

impl MetadataHub {
    fn new(info: Value) -> Self {
        Self::reply(StatusCode::OK, Bytes::from(info.to_string()))
    }

    fn reply(status: StatusCode, metadata: Bytes) -> Self {
        Self {
            metadata,
            status,
            files: MockHub::default(),
            requests: Arc::default(),
        }
    }

    fn request_count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }

    async fn service_with(transport: impl HttpTransport) -> HuggingFaceService {
        let mut service = HuggingFaceService::new(
            HuggingFaceOptions::default()
                .with_endpoint("https://hub.invalid")
                .with_token(TOKEN)
                .with_http_downloads(),
        )
        .await
        .unwrap();
        service.context =
            OperationContext::new().with_http_transport(HttpTransporter::new(transport));
        service
    }

    async fn service(&self) -> HuggingFaceService {
        Self::service_with(self.clone()).await
    }

    async fn resolve(&self, config: &str, split: &str) -> Result<HuggingFaceDataset> {
        self.service().await.resolve(selection(config, split)).await
    }
}

impl HttpTransport for MetadataHub {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        let uri = request.uri().to_string();
        self.requests.lock().unwrap().push(uri.clone());
        assert_eq!(
            request.headers()["authorization"],
            format!("Bearer {TOKEN}")
        );
        if uri.contains("/revision/") {
            return Ok(response(self.status, self.metadata.clone()));
        }
        assert!(
            uri.contains(COMMIT),
            "file I/O must use the resolved commit: {uri}"
        );
        self.files.fetch(request).await
    }
}

fn selection(config: &str, split: &str) -> HuggingFaceSelection {
    HuggingFaceSelection::new("owner/example", config, split)
}

fn info(data_files: Value) -> Value {
    json!({
        "sha": COMMIT,
        "cardData": {"configs": [{"config_name": "subset", "data_dir": "data", "data_files": data_files}]},
        "siblings": [{"rfilename":"data/b.parquet"}, {"rfilename":"data/a.parquet"}, {"rfilename":"data/test.parquet"}]
    })
}

#[tokio::test]
async fn resolves_revision_config_split_globs_and_reads_pinned_parquet() {
    let mut hub = MetadataHub::new(info(json!([
        {"split":"train", "path":["b.parquet", "[ab].parquet"]},
        {"split":"test", "path":"test.parquet"}
    ])));
    let fixture =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../testdata/parquet/ids.parquet");
    hub.files = MockHub::file(
        "b.parquet",
        Bytes::from(tokio::fs::read(fixture).await.unwrap()),
    );
    let dataset = hub
        .service()
        .await
        .resolve(selection("subset", "train").with_revision("refs/heads/topic"))
        .await
        .unwrap();
    assert_eq!(dataset.commit, COMMIT);
    assert_eq!(
        dataset.objects,
        ["data/b.parquet", "data/a.parquet"].map(ObjectRef::from)
    );
    assert!(hub.requests.lock().unwrap()[0].ends_with("/revision/refs%2Fheads%2Ftopic"));
    assert_eq!(
        hub.request_count(),
        1,
        "discovery must not download data files"
    );

    let format = open_arrow_access_file_format::ParquetAccessor::new(dataset.accessor);
    let mut reader = format
        .open(ReadRequest::new(dataset.objects[0].clone()))
        .await
        .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[1, 2, 3]
    );
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn string_and_path_list_declarations_select_train_without_guessing() {
    for (declaration, expected) in [
        (json!("a.parquet"), vec!["data/a.parquet"]),
        (
            json!(["b.parquet", "a.parquet"]),
            vec!["data/b.parquet", "data/a.parquet"],
        ),
    ] {
        let mut metadata = info(declaration);
        metadata["cardData"]["configs"]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "config_name": "unselected", "data_files": {"custom": "layout"}
            }));
        let hub = MetadataHub::new(metadata);
        let dataset = hub.resolve("subset", "train").await.unwrap();
        assert_eq!(
            dataset.objects,
            expected
                .into_iter()
                .map(ObjectRef::from)
                .collect::<Vec<_>>()
        );
        let error = hub.resolve("subset", "test").await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);
    }
}

#[tokio::test]
async fn resolves_the_checked_in_official_hub_metadata_fixture() {
    let fixture = Bytes::from_static(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../testdata/huggingface/doc-yaml-3.json"
    )));
    let hub = MetadataHub::reply(StatusCode::OK, fixture);
    let dataset = hub
        .service()
        .await
        .resolve(HuggingFaceSelection::new(
            "datasets-examples/doc-yaml-3",
            "default",
            "train",
        ))
        .await
        .unwrap();
    assert_eq!(dataset.commit, "2bb86c4a45b9e98af9e95d8c1ce76e3b7578218f");
    assert_eq!(
        dataset.objects,
        ["data/abc.csv", "data/edf.csv"].map(ObjectRef::from)
    );
}

#[tokio::test]
async fn rejects_missing_partial_unsupported_and_malformed_selections() {
    for (config, split) in [("missing", "train"), ("subset", "test")] {
        let hub = MetadataHub::new(info(json!("a.parquet")));
        assert_eq!(
            hub.resolve(config, split).await.unwrap_err().kind(),
            ErrorKind::NotFound
        );
        assert_eq!(hub.request_count(), 1);
    }
    for (metadata, expected) in [
        (
            info(json!(["a.parquet", "missing.parquet"])),
            ErrorKind::NotFound,
        ),
        (info(json!("../outside.parquet")), ErrorKind::InvalidInput),
        (info(json!("[bad")), ErrorKind::Decode),
        (
            info(json!("https://external.invalid/data.parquet")),
            ErrorKind::Unsupported,
        ),
        (
            json!({"sha":COMMIT,"siblings":[],"cardData":{"configs":[{"config_name":"subset"},{"config_name":"subset"}]}}),
            ErrorKind::Decode,
        ),
        (json!({"sha":COMMIT,"siblings":[]}), ErrorKind::Unsupported),
        (
            json!({"sha":COMMIT,"siblings":[],"cardData":{"configs":[{"config_name":"subset","data_dir":"data"}]}}),
            ErrorKind::Unsupported,
        ),
        (json!({"sha":"main","siblings":[]}), ErrorKind::Decode),
    ] {
        let hub = MetadataHub::new(metadata);
        let error = hub.resolve("subset", "train").await.unwrap_err();
        assert_eq!(error.kind(), expected, "{error:?}");
        assert_eq!(
            hub.request_count(),
            1,
            "failed discovery must not read files"
        );
        assert!(!format!("{error:?}").contains(TOKEN));
    }
}

#[tokio::test]
async fn metadata_errors_preserve_categories_and_credentials_are_redacted() {
    for (status, expected, upstream) in HTTP_ERRORS {
        let hub = MetadataHub::reply(status, Bytes::new());
        let error = hub.resolve("subset", "train").await.unwrap_err();
        assert_upstream_error(&error, expected, upstream, "resolve_huggingface");
    }
    let hub = MetadataHub::reply(StatusCode::OK, Bytes::from_static(b"{truncated"));
    assert_eq!(
        hub.resolve("subset", "train").await.unwrap_err().kind(),
        ErrorKind::Decode
    );
}

#[tokio::test]
async fn invalid_service_configuration_fails_before_network_io() {
    for options in [
        HuggingFaceOptions::default().with_token(""),
        HuggingFaceOptions::default().with_token("invalid\ntoken"),
        HuggingFaceOptions::default().with_endpoint("file:///tmp/hub"),
        HuggingFaceOptions::default().with_endpoint("https://user:secret@hub.invalid"),
    ] {
        assert_eq!(
            HuggingFaceService::new(options).await.unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
    let hub = MetadataHub::new(json!({}));
    let service = hub.service().await;
    for selection in [
        HuggingFaceSelection::new("../repo", "subset", "train"),
        selection("", "train"),
        selection("subset", "train").with_revision(".."),
    ] {
        assert_eq!(
            service.resolve(selection).await.unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
    assert_eq!(hub.request_count(), 0);
}

#[tokio::test]
async fn oversized_metadata_is_rejected_before_parsing_or_reading_files() {
    struct Oversized;
    impl HttpTransport for Oversized {
        async fn fetch(&self, _: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
            let chunk = Buffer::from(vec![b' '; 1024 * 1024]);
            let chunks = std::iter::repeat_n(chunk, 33).map(Ok);
            Ok(Response::new(HttpBody::new(stream::iter(chunks), None)))
        }
    }
    let service = MetadataHub::service_with(Oversized).await;
    let error = service
        .resolve(selection("subset", "train"))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
}

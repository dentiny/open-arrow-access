use std::{
    error::Error as _,
    sync::{Arc, Mutex},
};

use futures::stream;
use http::{Request, Response, StatusCode};
use open_arrow_access_core::{Accessor, Bytes, ErrorKind, ObjectRef, ReadRequest};
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter, OperationContext};
use serde_json::json;

use super::support::{MockHub, TOKEN, response};
use crate::huggingface::{HuggingFaceOptions, HuggingFaceSelection, HuggingFaceService};

const COMMIT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[derive(Clone)]
struct MetadataHub {
    metadata: Bytes,
    status: StatusCode,
    files: MockHub,
    requests: Arc<Mutex<Vec<String>>>,
}

impl MetadataHub {
    fn new(info: serde_json::Value) -> Self {
        Self {
            metadata: Bytes::from(info.to_string()),
            status: StatusCode::OK,
            files: MockHub::default(),
            requests: Arc::default(),
        }
    }

    async fn service(&self) -> HuggingFaceService {
        let mut service = HuggingFaceService::new(
            HuggingFaceOptions::default()
                .with_endpoint("https://hub.invalid")
                .with_token(TOKEN)
                .with_http_downloads(),
        )
        .await
        .unwrap();
        service.context =
            OperationContext::new().with_http_transport(HttpTransporter::new(self.clone()));
        service
    }
}

impl HttpTransport for MetadataHub {
    async fn fetch(&self, mut request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
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
            "resolved file I/O must use the pinned commit: {uri}"
        );
        *request.uri_mut() = uri
            .replace(COMMIT, "refs%2Fconvert%2Fparquet")
            .parse()
            .unwrap();
        self.files.fetch(request).await
    }
}

fn info(data_files: serde_json::Value) -> serde_json::Value {
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
    hub.files = MockHub::with_files([(
        "data/b.parquet".into(),
        Bytes::from(std::fs::read(fixture).unwrap()),
    )]);
    let selection = HuggingFaceSelection::new("owner/example", "subset", "train")
        .with_revision("refs/heads/topic");
    let dataset = hub.service().await.resolve(selection).await.unwrap();
    assert_eq!(dataset.commit, COMMIT);
    assert_eq!(
        dataset
            .objects
            .iter()
            .map(ObjectRef::locator)
            .collect::<Vec<_>>(),
        ["data/b.parquet", "data/a.parquet"]
    );
    assert!(hub.requests.lock().unwrap()[0].ends_with("/revision/refs%2Fheads%2Ftopic"));
    assert_eq!(
        hub.requests.lock().unwrap().len(),
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
    for declaration in [json!("a.parquet"), json!(["b.parquet", "a.parquet"])] {
        let mut metadata = info(declaration);
        metadata["cardData"]["configs"]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "config_name": "unselected", "data_files": {"custom": "layout"}
            }));
        let hub = MetadataHub::new(metadata);
        let service = hub.service().await;
        let dataset = service
            .resolve(HuggingFaceSelection::new(
                "owner/example",
                "subset",
                "train",
            ))
            .await
            .unwrap();
        assert!(!dataset.objects.is_empty());
        let error = service
            .resolve(HuggingFaceSelection::new("owner/example", "subset", "test"))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);
    }
}

#[tokio::test]
async fn resolves_the_checked_in_official_hub_metadata_fixture() {
    let fixture = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../testdata/huggingface/doc-yaml-3.json"
    ));
    let hub = MetadataHub::new(serde_json::from_str(fixture).unwrap());
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
        dataset
            .objects
            .iter()
            .map(ObjectRef::locator)
            .collect::<Vec<_>>(),
        ["data/abc.csv", "data/edf.csv"]
    );
}

#[tokio::test]
async fn rejects_missing_partial_unsupported_and_malformed_selections() {
    for (metadata, config, split, expected) in [
        (
            info(json!("a.parquet")),
            "missing",
            "train",
            ErrorKind::NotFound,
        ),
        (
            info(json!("a.parquet")),
            "subset",
            "test",
            ErrorKind::NotFound,
        ),
        (
            info(json!(["a.parquet", "missing.parquet"])),
            "subset",
            "train",
            ErrorKind::NotFound,
        ),
        (
            info(json!("../outside.parquet")),
            "subset",
            "train",
            ErrorKind::InvalidInput,
        ),
        (info(json!("[bad")), "subset", "train", ErrorKind::Decode),
        (
            info(json!("https://external.invalid/data.parquet")),
            "subset",
            "train",
            ErrorKind::Unsupported,
        ),
        (
            json!({"sha":COMMIT,"siblings":[],"cardData":{"configs":[{"config_name":"subset"},{"config_name":"subset"}]}}),
            "subset",
            "train",
            ErrorKind::Decode,
        ),
        (
            json!({"sha":COMMIT,"siblings":[]}),
            "subset",
            "train",
            ErrorKind::Unsupported,
        ),
        (
            json!({"sha":COMMIT,"siblings":[],"cardData":{"configs":[{"config_name":"subset","data_dir":"data"}]}}),
            "subset",
            "train",
            ErrorKind::Unsupported,
        ),
        (
            json!({"sha":"main","siblings":[]}),
            "subset",
            "train",
            ErrorKind::Decode,
        ),
    ] {
        let hub = MetadataHub::new(metadata);
        let error = hub
            .service()
            .await
            .resolve(HuggingFaceSelection::new("owner/example", config, split))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), expected, "{error:?}");
        assert_eq!(
            hub.requests.lock().unwrap().len(),
            1,
            "failed discovery must not read repository files"
        );
        assert!(!format!("{error:?}").contains(TOKEN));
    }
}

#[tokio::test]
async fn metadata_errors_preserve_categories_and_credentials_are_redacted() {
    for (status, expected) in [
        (StatusCode::NOT_FOUND, ErrorKind::NotFound),
        (StatusCode::UNAUTHORIZED, ErrorKind::PermissionDenied),
        (StatusCode::FORBIDDEN, ErrorKind::PermissionDenied),
        (StatusCode::TOO_MANY_REQUESTS, ErrorKind::Io),
        (StatusCode::INTERNAL_SERVER_ERROR, ErrorKind::Io),
    ] {
        let mut hub = MetadataHub::new(json!({}));
        hub.status = status;
        let error = hub
            .service()
            .await
            .resolve(HuggingFaceSelection::new(
                "owner/example",
                "subset",
                "train",
            ))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), expected);
        assert!(
            error
                .source()
                .unwrap()
                .downcast_ref::<opendal::Error>()
                .is_some()
        );
        assert!(!format!("{error:?}").contains(TOKEN));
    }
    let mut hub = MetadataHub::new(json!({}));
    hub.metadata = Bytes::from_static(b"{truncated");
    let error = hub
        .service()
        .await
        .resolve(HuggingFaceSelection::new(
            "owner/example",
            "subset",
            "train",
        ))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Decode);
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
        HuggingFaceSelection::new("owner/example", "", "train"),
        HuggingFaceSelection::new("owner/example", "subset", "train").with_revision(".."),
    ] {
        assert_eq!(
            service.resolve(selection).await.unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
    assert!(hub.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn oversized_metadata_is_rejected_before_parsing_or_reading_files() {
    #[derive(Clone)]
    struct Oversized;
    impl HttpTransport for Oversized {
        async fn fetch(&self, _: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
            let bytes = Bytes::from(vec![b' '; 32 * 1024 * 1024 + 1]);
            Ok(Response::new(HttpBody::new(
                stream::iter([Ok(Buffer::from(bytes))]),
                None,
            )))
        }
    }
    let mut service = MetadataHub::new(json!({})).service().await;
    service.context = OperationContext::new().with_http_transport(HttpTransporter::new(Oversized));
    let error = service
        .resolve(HuggingFaceSelection::new(
            "owner/example",
            "subset",
            "train",
        ))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
}

mod providers;
use http::StatusCode;
use open_arrow_access_core::{Bytes, ErrorKind};
use open_arrow_access_service::semantic_scholar::SemanticScholarService;
use open_arrow_access_test_support::{byte_accessor, fixture, http::HttpFixture};

#[tokio::test]
async fn pins_latest_and_preserves_signed_partitions_without_leaking_key() {
    let f = HttpFixture::new();
    let download = HttpFixture::new();
    f.respond(
        "/datasets/v1/release/latest",
        StatusCode::OK,
        fixture!("semantic-scholar", "release.json").await,
    );
    let raw = String::from_utf8(fixture!("semantic-scholar", "dataset.json").await)
        .unwrap()
        .replace("ORIGIN", download.endpoint());
    f.respond(
        "/datasets/v1/release/2026-10-27/dataset/s2orc_v2",
        StatusCode::OK,
        raw.into_bytes(),
    );
    download.file("/part?signature=a%2Bb", Bytes::from_static(b"raw-data"));
    let s = SemanticScholarService::with_connection(
        providers::connection(&f)
            .with_header("x-api-key", "test")
            .unwrap(),
    );
    let files = s.dataset("latest", "s2orc_v2").await.unwrap();
    assert_eq!(files.metadata().release_id, "2026-10-27");
    assert_eq!(files.metadata().dataset.value()["README"], "source license");
    assert_eq!(f.requests().len(), 2);
    assert!(download.requests().is_empty());
    byte_accessor::assert_read_contract(files, "part-00000".into(), b"raw-data")
        .await
        .unwrap();
    assert!(!download.requests()[0].headers.contains_key("x-api-key"));
}

#[tokio::test]
async fn validates_identity_and_required_arrays() {
    let f = HttpFixture::new();
    f.respond(
        "/datasets/v1/release/2026-10-27",
        StatusCode::OK,
        fixture!("semantic-scholar", "release.json").await,
    );
    f.respond(
        "/datasets/v1/release/2026-10-27/dataset/s2orc_v2",
        StatusCode::OK,
        fixture!("semantic-scholar", "bad-dataset-identity.json").await,
    );
    let s = SemanticScholarService::with_connection(providers::connection(&f));
    assert_eq!(
        s.dataset("2026-10-27", "s2orc_v2")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Decode
    );
    assert!(SemanticScholarService::new("").is_err());
}

#[tokio::test]
async fn fetches_only_the_pinned_dataset_manifest_and_validates_it() {
    let f = HttpFixture::new();
    f.respond(
        "/datasets/v1/release/2026-10-27/dataset/s2orc_v2",
        StatusCode::OK,
        fixture!("semantic-scholar", "dataset.json").await,
    );
    let service = SemanticScholarService::with_connection(providers::connection(&f));
    assert!(
        service
            .dataset_manifest("latest", "s2orc_v2")
            .await
            .is_err()
    );
    assert!(service.dataset_manifest("", "s2orc_v2").await.is_err());
    assert!(f.requests().is_empty());
    let manifest = service
        .dataset_manifest("2026-10-27", "s2orc_v2")
        .await
        .unwrap();
    assert_eq!(manifest.value()["README"], "source license");
    assert_eq!(f.requests().len(), 1);
    f.respond(
        "/datasets/v1/release/2026-10-27/dataset/s2orc_v2",
        StatusCode::OK,
        fixture!("semantic-scholar", "bad-dataset-identity.json").await,
    );
    assert_eq!(
        service
            .dataset_manifest("2026-10-27", "s2orc_v2")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Decode
    );
}

#[test]
fn binds_existing_manifests_without_api_requests_and_revalidates_identity() {
    use open_arrow_access_service::{common::ApiDocument, semantic_scholar::DatasetMetadata};
    fn metadata(release_id: &str, dataset: &str, url: &str) -> DatasetMetadata {
        DatasetMetadata {
            release_id: release_id.into(),
            release: ApiDocument::from_bytes(Bytes::from_static(
                br#"{"release_id":"2026-10-27","datasets":["s2orc_v2"]}"#,
            ))
            .unwrap(),
            dataset: ApiDocument::from_bytes(Bytes::from(
                serde_json::to_vec(
                    &serde_json::json!({"name":dataset,"files":[url],"unknown":"retained"}),
                )
                .unwrap(),
            ))
            .unwrap(),
        }
    }
    let service = SemanticScholarService::new("fixture-key").unwrap();
    let url = "https://download.invalid/part?signature=a%2Bb%2F";
    let files = service
        .files_from_manifest("s2orc_v2", metadata("2026-10-27", "s2orc_v2", url))
        .unwrap();
    assert_eq!(files.metadata().dataset.value()["files"][0], url);
    assert_eq!(files.metadata().dataset.value()["unknown"], "retained");
    for invalid in [
        metadata("latest", "s2orc_v2", url),
        metadata("2026-10-28", "s2orc_v2", url),
        metadata("2026-10-27", "other", url),
        metadata("2026-10-27", "s2orc_v2", "file:///private/data"),
    ] {
        assert!(service.files_from_manifest("s2orc_v2", invalid).is_err());
    }
}

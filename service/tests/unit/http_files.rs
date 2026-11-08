use std::error::Error as _;

use futures::TryStreamExt;
use http::StatusCode;
use open_arrow_access_core::{ByteAccessor, Error, ErrorKind, ListRequest};
use open_arrow_access_test_support::{fixture, http::HttpFixture};
use serde_json::Value;

use super::{HttpFileService, ResolvedFilesPlan};

fn membership_error(error: Error) -> Error {
    Error::new(
        ErrorKind::Decode,
        "resolve_fixture",
        "invalid fixture membership",
    )
    .with_source(error)
}

fn service(http: &HttpFixture) -> HttpFileService {
    HttpFileService::new(
        http.endpoint(),
        "configure_fixture",
        "resolve_fixture",
        membership_error,
    )
    .unwrap()
}

#[test]
fn invalid_origins_keep_the_provider_configuration_operation() {
    for endpoint in [
        "ftp://example.invalid",
        "https://user:pass@example.invalid",
        "https://example.invalid/api",
        "https://example.invalid?token=x",
        "https://example.invalid/#fragment",
    ] {
        let error = HttpFileService::new(
            endpoint,
            "configure_fixture",
            "resolve_fixture",
            membership_error,
        )
        .err()
        .unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.operation(), "configure_fixture");
    }
}

#[tokio::test]
async fn http_and_json_failures_skip_provider_mapping() {
    for (status, file, kind) in [
        (
            StatusCode::FORBIDDEN,
            "record.json",
            ErrorKind::PermissionDenied,
        ),
        (StatusCode::OK, "malformed.json", ErrorKind::Decode),
    ] {
        let http = HttpFixture::new();
        http.respond("/manifest", status, fixture!("zenodo", file).await);
        let service = service(&http);
        let error = service
            .resolve_files::<Value, ()>(&service.endpoint().join("manifest").unwrap(), |_| {
                panic!("mapping must not run after a fetch/decode failure")
            })
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), kind);
        assert_eq!(error.operation(), "resolve_fixture");
        assert!(error.source().is_some());
        assert_eq!(http.requests().len(), 1);
    }
}

#[tokio::test]
async fn provider_validation_errors_pass_through_without_opening_files() {
    let http = HttpFixture::new();
    http.respond(
        "/manifest",
        StatusCode::OK,
        fixture!("zenodo", "record.json").await,
    );
    let service = service(&http);
    let error = service
        .resolve_files::<Value, ()>(&service.endpoint().join("manifest").unwrap(), |_| {
            Err(
                Error::new(ErrorKind::Unsupported, "validate_fixture", "not published")
                    .with_source(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "fixture cause",
                    )),
            )
        })
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert_eq!(error.operation(), "validate_fixture");
    assert_eq!(error.to_string(), "validate_fixture: not published");
    let cause = error
        .source()
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
    assert_eq!(cause.kind(), std::io::ErrorKind::PermissionDenied);
    assert_eq!(cause.to_string(), "fixture cause");
    assert_eq!(http.requests().len(), 1);
}

#[tokio::test]
async fn namespace_errors_keep_provider_context_and_empty_membership_is_valid() {
    for names in [vec!["duplicate", "duplicate"], vec![""]] {
        let http = HttpFixture::new();
        http.respond(
            "/manifest",
            StatusCode::OK,
            fixture!("zenodo", "record.json").await,
        );
        let service = service(&http);
        let error = service
            .resolve_files(&service.endpoint().join("manifest").unwrap(), |_: Value| {
                Ok(ResolvedFilesPlan {
                    root: "downloads/".into(),
                    files: names
                        .into_iter()
                        .map(|name| (name.into(), "object".into()))
                        .collect(),
                    metadata: (),
                })
            })
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::Decode);
        assert_eq!(error.operation(), "resolve_fixture");
        let cause = error.source().unwrap().downcast_ref::<Error>().unwrap();
        assert_eq!(cause.kind(), ErrorKind::InvalidInput);
        assert_eq!(cause.operation(), "configure_named_files");
        assert_eq!(http.requests().len(), 1);
    }
    let http = HttpFixture::new();
    http.respond(
        "/manifest",
        StatusCode::OK,
        fixture!("zenodo", "empty.json").await,
    );
    let service = service(&http);
    let files = service
        .resolve_files(&service.endpoint().join("manifest").unwrap(), |_: Value| {
            Ok(ResolvedFilesPlan {
                root: "downloads/".into(),
                files: vec![],
                metadata: "empty fixture",
            })
        })
        .await
        .unwrap();
    assert_eq!(files.metadata(), &"empty fixture");
    assert!(
        files
            .list(ListRequest::new(""))
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(http.requests().len(), 1);
}

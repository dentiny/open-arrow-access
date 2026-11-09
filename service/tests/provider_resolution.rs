use std::{error::Error as _, io};

use futures::TryStreamExt;
use http::StatusCode;
use open_arrow_access_core::{ByteAccessor, Error, ErrorKind, ListRequest, ObjectRef};
use open_arrow_access_service::common::{LinkField, resolve_optional_links};
use open_arrow_access_test_support::http::HttpFixture;

mod providers;

#[tokio::test]
async fn resolution_preserves_query_and_metadata_without_downloading_files() {
    let api = HttpFixture::new();
    let downloads = HttpFixture::new();
    api.respond(
        "/manifest?version=4",
        StatusCode::OK,
        format!(
            r#"{{"pdf_url":"{}/paper.pdf","unknown":"retained"}}"#,
            downloads.endpoint()
        ),
    );
    let files = providers::connection(&api)
        .resolve_json_files("manifest", &[("version", "4")], |doc| {
            resolve_optional_links(
                doc.value(),
                &[LinkField {
                    json_field: "pdf_url",
                    file_name: "paper.pdf",
                }],
            )
        })
        .await
        .unwrap();
    assert_eq!(files.metadata().value()["unknown"], "retained");
    let names = files
        .list(ListRequest::new(""))
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(names, vec![ObjectRef::new("paper.pdf")]);
    assert_eq!(api.requests().len(), 1);
    assert!(downloads.requests().is_empty());
}

#[tokio::test]
async fn resolver_errors_are_preserved() {
    let api = HttpFixture::new();
    api.respond("/manifest", StatusCode::OK, "{}");
    let error = providers::connection(&api)
        .resolve_json_files("manifest", &[], |_| {
            Err(Error::new(
                ErrorKind::PermissionDenied,
                "validate_provider",
                "restricted",
            )
            .with_source(io::Error::other("original cause")))
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert_eq!(error.operation(), "validate_provider");
    assert_eq!(error.source().unwrap().to_string(), "original cause");
}

mod providers;

use std::{
    error::Error as _,
    sync::atomic::{AtomicUsize, Ordering},
};

use futures::TryStreamExt;
use http::StatusCode;
use open_arrow_access_core::{Accessor, Bytes, Error, ErrorKind, ListRequest};
use open_arrow_access_service::common::{LinkField, MetadataLink, resolve_optional_links};
use open_arrow_access_test_support::{fixture, http::HttpFixture};

#[tokio::test]
async fn resolution_retains_document_and_query_without_downloading_files() {
    let api = HttpFixture::new();
    let downloads = HttpFixture::new();
    let raw = String::from_utf8(fixture!("federal-register", "document.json").await)
        .unwrap()
        .replace("ORIGIN", downloads.endpoint());
    api.respond("/manifest?version=4", StatusCode::OK, raw.into_bytes());
    downloads.file("/paper.pdf", Bytes::from_static(b"original"));
    let calls = AtomicUsize::new(0);
    let files = providers::connection(&api)
        .resolve_json_files("manifest", &[("version", "4")], |doc| {
            calls.fetch_add(1, Ordering::SeqCst);
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
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(files.metadata().value()["unknown"], "retained");
    let names = open_arrow_access_core::ByteAccessor::list(&files, ListRequest::new(""))
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(names[0].locator(), "paper.pdf");
    assert_eq!(api.requests().len(), 1);
    assert!(downloads.requests().is_empty());
    let reader = files.open("paper.pdf".into()).await.unwrap();
    assert_eq!(
        open_arrow_access_core::read_all(reader, "test")
            .await
            .unwrap(),
        b"original"
    );
}

#[tokio::test]
async fn resolver_errors_are_preserved_and_namespace_validation_precedes_downloads() {
    let api = HttpFixture::new();
    api.respond(
        "/manifest",
        StatusCode::OK,
        fixture!("federal-register", "document.json").await,
    );
    let connection = providers::connection(&api);
    let error = connection
        .resolve_json_files("manifest", &[], |_| {
            Err(Error::new(
                ErrorKind::PermissionDenied,
                "validate_provider",
                "restricted",
            )
            .with_source(std::io::Error::other("original cause")))
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert_eq!(error.operation(), "validate_provider");
    assert_eq!(error.source().unwrap().to_string(), "original cause");
    for urls in [
        vec!["file:///invalid"],
        vec!["https://example.invalid/a", "https://example.invalid/b"],
    ] {
        let error = connection
            .resolve_json_files("manifest", &[], |_| {
                Ok(urls
                    .into_iter()
                    .map(|url| MetadataLink {
                        file_name: "same-name".into(),
                        url: url.into(),
                    })
                    .collect())
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Decode);
    }
    assert_eq!(api.requests().len(), 3);
}

#[tokio::test]
async fn http_failures_skip_resolver_and_empty_membership_is_valid() {
    let api = HttpFixture::new();
    api.respond("/missing", StatusCode::NOT_FOUND, Bytes::new());
    let connection = providers::connection(&api);
    let error = connection
        .resolve_json_files("missing", &[], |_| {
            panic!("resolver must not run after HTTP failure")
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::NotFound);
    api.respond(
        "/empty",
        StatusCode::OK,
        fixture!("federal-register", "document.json").await,
    );
    let files = connection
        .resolve_json_files("empty", &[], |_| Ok(vec![]))
        .await
        .unwrap();
    assert!(
        open_arrow_access_core::ByteAccessor::list(&files, ListRequest::new(""))
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(api.requests().len(), 2);
}

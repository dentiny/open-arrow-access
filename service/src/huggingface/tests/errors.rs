use std::num::NonZeroUsize;

use futures::TryStreamExt;
use http::StatusCode as Status;
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadOptions, ByteReadRequest, Bytes, ErrorKind,
};
use opendal::ErrorKind as Upstream;

use super::support::{HTTP_ERRORS, HubFailure, MockHub, assert_upstream_error};

#[tokio::test]
async fn missing_denied_and_transport_errors_retain_kind_and_terminate() {
    let hub = MockHub::file("fail.bin", Bytes::from_static(b"abcdefgh"));
    let accessor = hub
        .accessor()
        .with_chunk_size(NonZeroUsize::new(4).unwrap());
    let error = accessor.open("missing.bin".into()).await.err().unwrap();
    assert_upstream_error(&error, ErrorKind::NotFound, Upstream::NotFound, "stat");
    let error = accessor.open("denied.bin".into()).await.err().unwrap();
    assert_upstream_error(
        &error,
        ErrorKind::PermissionDenied,
        Upstream::PermissionDenied,
        "stat",
    );
    let mut reader = accessor
        .open(
            ByteReadRequest::new("fail.bin").with_options(ByteReadOptions {
                io_chunk_size: NonZeroUsize::new(4),
                ..ByteReadOptions::default()
            }),
        )
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), b"abcd");
    let error = reader.next().await.err().unwrap();
    assert_upstream_error(&error, ErrorKind::Io, Upstream::Unexpected, "read");
    let before = hub.request_count();
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(hub.request_count(), before);
}

#[tokio::test]
async fn metadata_http_status_codes_preserve_classification_and_source() {
    for (status, expected, upstream) in HTTP_ERRORS {
        let hub = MockHub::default().with_failure("/paths-info/", HubFailure::Status(status));
        let accessor = hub.accessor();
        let error = accessor.open("file.bin".into()).await.err().unwrap();
        assert_upstream_error(&error, expected, upstream, "stat");
        let error = accessor.list("nested".into()).await.err().unwrap();
        assert_upstream_error(&error, expected, upstream, "list");
        assert!(
            hub.captured()
                .iter()
                .all(|request| request.uri.contains("/paths-info/"))
        );
    }
}

#[tokio::test]
async fn read_http_status_codes_preserve_source_and_stop_sequential_reads() {
    for (status, expected, upstream) in HTTP_ERRORS {
        let hub = MockHub::file("file.bin", Bytes::from_static(b"abcdefgh"))
            .with_failure("/resolve/", HubFailure::Status(status));
        let accessor = hub.accessor();
        let mut reader = accessor.open("file.bin".into()).await.unwrap();
        let error = reader.read_range(0..2).await.unwrap_err();
        assert_upstream_error(&error, expected, upstream, "read");
        let error = reader.read_ranges(vec![0..1, 4..5]).await.unwrap_err();
        assert_upstream_error(&error, expected, upstream, "read_ranges");
        let error = reader.next().await.unwrap_err();
        assert_upstream_error(&error, expected, upstream, "read");
        let calls = hub.request_count();
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(hub.request_count(), calls);
    }
}

#[tokio::test]
async fn transport_and_unsupported_failures_retain_original_categories() {
    for (upstream, expected) in [
        (Upstream::Unexpected, ErrorKind::Io),
        (Upstream::Unsupported, ErrorKind::Unsupported),
    ] {
        let hub = MockHub::file("file.bin", Bytes::from_static(b"payload"))
            .with_failure("/resolve/", HubFailure::Transport(upstream));
        let mut reader = hub.accessor().open("file.bin".into()).await.unwrap();
        let error = reader.next().await.unwrap_err();
        assert_upstream_error(&error, expected, upstream, "read");
        let calls = hub.request_count();
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(hub.request_count(), calls);
    }
}

#[tokio::test]
async fn listing_http_failures_preserve_categories_and_sources() {
    for (status, expected, upstream) in [
        (
            Status::FORBIDDEN,
            ErrorKind::PermissionDenied,
            Upstream::PermissionDenied,
        ),
        (
            Status::INTERNAL_SERVER_ERROR,
            ErrorKind::Io,
            Upstream::Unexpected,
        ),
    ] {
        let hub = MockHub::default().with_failure("/tree/", HubFailure::Status(status));
        let mut objects = hub.accessor().list("".into()).await.unwrap();
        let error = objects.try_next().await.unwrap_err();
        assert_upstream_error(&error, expected, upstream, "list");
        let calls = hub.request_count();
        assert!(objects.try_next().await.unwrap().is_none());
        assert_eq!(hub.request_count(), calls);
    }
}

use std::num::{NonZeroU64, NonZeroUsize};

use futures::TryStreamExt;
use http::StatusCode;
use open_arrow_access_core::{
    ByteAccessor, ByteReadOptions, ByteReadRequest, Bytes, ErrorKind, ListRequest,
};
use open_arrow_access_service::zenodo::ZenodoService;
use open_arrow_access_test_support::{
    byte_accessor, fixture,
    http::{HttpFixture, assert_resolution_cases},
};

const ID: NonZeroU64 = NonZeroU64::new(3551612).unwrap();

async fn fixture_service(name: &str) -> (HttpFixture, ZenodoService) {
    let fixture = HttpFixture::new();
    fixture.respond(
        "/api/records/3551612",
        StatusCode::OK,
        Bytes::from(fixture!("zenodo", &format!("{name}.json")).await),
    );
    fixture.file(
        "/api/records/3551612/files/paper%20%231%25.pdf/content",
        Bytes::from(fixture!("zenodo", "attachment.bin").await),
    );
    let service = ZenodoService::with_endpoint(fixture.endpoint()).unwrap();
    (fixture, service)
}

#[tokio::test]
async fn record_membership_and_lazy_independent_file_reads() {
    let (fixture, service) = fixture_service("record").await;
    let accessor = service.record(ID).await.unwrap();
    assert_eq!(fixture.requests().len(), 1);
    assert_eq!(accessor.id(), ID);
    assert_eq!(accessor.files()[0].size, 8);
    assert_eq!(accessor.files()[0].checksum, "md5:fixture");
    let listed: Vec<_> = accessor
        .list(ListRequest::new(""))
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(listed[0].locator(), "paper #1%.pdf");
    assert_eq!(fixture.requests().len(), 1, "listing does not fetch files");
    let options = ByteReadOptions {
        io_chunk_size: Some(NonZeroUsize::new(2).unwrap()),
        ..Default::default()
    };
    byte_accessor::assert_ranged_read_contract(
        accessor,
        ByteReadRequest::new("paper #1%.pdf").with_options(options),
        b"%PDFdata",
        &[1..4, 0..0, 1..4],
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn rejects_invalid_metadata_and_maps_http_errors() {
    assert_resolution_cases(
        env!("CARGO_MANIFEST_DIR"),
        "zenodo",
        "/api/records/3551612",
        &[
            ("malformed.json", StatusCode::OK, ErrorKind::Decode),
            ("wrong-id.json", StatusCode::OK, ErrorKind::Decode),
            ("missing-files.json", StatusCode::OK, ErrorKind::Decode),
            ("duplicate.json", StatusCode::OK, ErrorKind::Decode),
            ("traversal.json", StatusCode::OK, ErrorKind::Decode),
            (
                "empty.json",
                StatusCode::FORBIDDEN,
                ErrorKind::PermissionDenied,
            ),
            (
                "empty.json",
                StatusCode::TOO_MANY_REQUESTS,
                ErrorKind::RateLimited,
            ),
        ],
        |endpoint| async move { ZenodoService::with_endpoint(&endpoint)?.record(ID).await },
    )
    .await;
    let (_fixture, service) = fixture_service("empty").await;
    assert!(service.record(ID).await.unwrap().files().is_empty());
}

#[tokio::test]
async fn record_metadata_can_exceed_the_previous_size_limit() {
    let (fixture, service) = fixture_service("record").await;
    let mut raw = fixture!("zenodo", "record.json").await;
    raw.extend(std::iter::repeat_n(b' ', 4 * 1024 * 1024));
    fixture.respond_chunked("/api/records/3551612", StatusCode::OK, Bytes::from(raw));
    assert!(!service.record(ID).await.unwrap().files().is_empty());
}

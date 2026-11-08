use futures::TryStreamExt;
use http::StatusCode;
use open_arrow_access_core::{
    ByteAccessor, ByteReadOptions, ByteReadRequest, Bytes, ErrorKind, ListRequest,
};
use open_arrow_access_service::internet_archive::InternetArchiveService;
use open_arrow_access_test_support::{
    byte_accessor, fixture,
    http::{HttpFixture, assert_resolution_cases},
};

const ITEM: &str = "sample-item";
const NAME: &str = "papers/paper #1%.pdf";
const METADATA: &str = "/metadata/sample-item";
const DOWNLOAD: &str = "/download/sample-item/papers/paper%20%231%25.pdf";

#[tokio::test]
async fn resolves_once_and_reads_nested_names_lazily_with_independent_readers() {
    let fixture = HttpFixture::new();
    fixture.respond(
        METADATA,
        StatusCode::OK,
        Bytes::from(fixture!("internet_archive", "item.json").await),
    );
    fixture.file(
        DOWNLOAD,
        Bytes::from(fixture!("internet_archive", "attachment.bin").await),
    );
    let item = InternetArchiveService::with_endpoint(fixture.endpoint())
        .unwrap()
        .item(ITEM)
        .await
        .unwrap();
    assert_eq!(item.identifier(), ITEM);
    assert_eq!(item.files()[0].size, Some(8));
    assert_eq!(item.files()[0].md5.as_deref(), Some("fixture-md5"));
    assert_eq!(item.files()[1].size, None);
    assert_eq!(item.files()[2].size, Some(0));
    let names: Vec<_> = item
        .list(ListRequest::new(""))
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        names.iter().map(|name| name.locator()).collect::<Vec<_>>(),
        vec![NAME, "notes.xml", "empty.txt"]
    );
    assert_eq!(fixture.requests().len(), 1, "listing does not fetch files");
    let options = ByteReadOptions {
        io_chunk_size: std::num::NonZeroUsize::new(2),
        ..Default::default()
    };
    byte_accessor::assert_ranged_read_contract(
        item,
        ByteReadRequest::new(NAME).with_options(options),
        b"%PDFdata",
        &[1..4, 0..0, 1..4],
    )
    .await
    .unwrap();
    let requests = fixture.requests();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.path == METADATA)
            .count(),
        1
    );
    assert!(
        requests
            .iter()
            .any(|request| request.headers.contains_key("range"))
    );
}

#[tokio::test]
async fn maps_empty_missing_and_unavailable_item_responses() {
    assert_resolution_cases(
        env!("CARGO_MANIFEST_DIR"),
        "internet_archive",
        METADATA,
        &[
            ("not-found.json", StatusCode::OK, ErrorKind::NotFound),
            ("api-error.json", StatusCode::OK, ErrorKind::Decode),
            ("dark.json", StatusCode::OK, ErrorKind::PermissionDenied),
            ("wrong-id.json", StatusCode::OK, ErrorKind::Decode),
            ("missing-files.json", StatusCode::OK, ErrorKind::Decode),
            ("duplicate.json", StatusCode::OK, ErrorKind::Decode),
            ("traversal.json", StatusCode::OK, ErrorKind::Decode),
            ("invalid-size.json", StatusCode::OK, ErrorKind::Decode),
        ],
        |endpoint| async move {
            InternetArchiveService::with_endpoint(&endpoint)?
                .item(ITEM)
                .await
        },
    )
    .await;
    let fixture = HttpFixture::new();
    fixture.respond(
        METADATA,
        StatusCode::OK,
        Bytes::from(fixture!("internet_archive", "empty.json").await),
    );
    let item = InternetArchiveService::with_endpoint(fixture.endpoint())
        .unwrap()
        .item(ITEM)
        .await
        .unwrap();
    assert!(item.files().is_empty());
}

#[tokio::test]
async fn validates_selection_before_io_and_reads_chunked_metadata() {
    let fixture = HttpFixture::new();
    let service = InternetArchiveService::with_endpoint(fixture.endpoint()).unwrap();
    for identifier in [
        "",
        "../item",
        "item/child",
        " item",
        "item?query",
        "item#fragment",
        "item\\child",
    ] {
        assert_eq!(
            service.item(identifier).await.err().unwrap().kind(),
            ErrorKind::InvalidInput
        );
    }
    assert_eq!(
        service.item(&"a".repeat(101)).await.err().unwrap().kind(),
        ErrorKind::InvalidInput
    );
    assert!(fixture.requests().is_empty());
    fixture.respond_chunked(
        METADATA,
        StatusCode::OK,
        Bytes::from(fixture!("internet_archive", "item.json").await),
    );
    assert!(service.item(ITEM).await.is_ok());
}

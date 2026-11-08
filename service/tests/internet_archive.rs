use futures::TryStreamExt;
use http::StatusCode;
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadOptions, ByteReadRequest, Bytes, ErrorKind, ListRequest,
};
use open_arrow_access_service::internet_archive::InternetArchiveService;
use open_arrow_access_test_support::{collect_bytes, fixture, http::HttpFixture};

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
    for recursive in [true, false] {
        let names: Vec<_> = item
            .list(ListRequest::new("").with_recursive(recursive))
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            names.iter().map(|name| name.locator()).collect::<Vec<_>>(),
            vec![NAME, "notes.xml", "empty.txt"]
        );
    }
    assert_eq!(
        item.open("absent.pdf".into()).await.err().unwrap().kind(),
        ErrorKind::NotFound
    );
    assert_eq!(
        fixture.requests().len(),
        1,
        "cached discovery and unknown names do not fetch files"
    );
    let options = ByteReadOptions {
        io_chunk_size: std::num::NonZeroUsize::new(2),
        ..Default::default()
    };
    let mut reader = item
        .open(ByteReadRequest::new(NAME).with_options(options))
        .await
        .unwrap();
    assert_eq!(reader.read_range(1..4).await.unwrap().as_ref(), b"PDF");
    let mut second = item.open(NAME.into()).await.unwrap();
    drop(item);
    assert_eq!(collect_bytes(reader.as_mut()).await.unwrap(), b"%PDFdata");
    assert_eq!(collect_bytes(second.as_mut()).await.unwrap(), b"%PDFdata");
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
    for (name, kind) in [
        ("not-found", ErrorKind::NotFound),
        ("api-error", ErrorKind::Decode),
        ("dark", ErrorKind::PermissionDenied),
        ("wrong-id", ErrorKind::Decode),
        ("missing-files", ErrorKind::Decode),
        ("duplicate", ErrorKind::Decode),
        ("traversal", ErrorKind::Decode),
        ("invalid-size", ErrorKind::Decode),
    ] {
        let fixture = HttpFixture::new();
        fixture.respond(
            METADATA,
            StatusCode::OK,
            Bytes::from(fixture!("internet_archive", &format!("{name}.json")).await),
        );
        let error = InternetArchiveService::with_endpoint(fixture.endpoint())
            .unwrap()
            .item(ITEM)
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), kind, "{name}");
        assert_eq!(fixture.requests().len(), 1);
    }
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

use futures::TryStreamExt;
use http::StatusCode;
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadOptions, ByteReadRequest, Bytes, ErrorKind, ListRequest,
    read_all,
};
use open_arrow_access_service::internet_archive::InternetArchiveService;
use open_arrow_access_test_support::{
    byte_accessor, fixture,
    http::{HttpFixture, assert_resolution_cases, response},
};

const ITEM: &str = "sample-item";
const NAME: &str = "papers/paper #1%.pdf";
const METADATA: &str = "/metadata/sample-item";
const DOWNLOAD: &str = "/download/sample-item/papers/paper%20%231%25.pdf";

#[tokio::test]
async fn authenticates_hidden_item_and_reads_files_lazily() {
    let fixture = HttpFixture::new();
    fixture.respond(
        METADATA,
        StatusCode::OK,
        Bytes::from(fixture!("internet_archive", "dark.json").await),
    );
    fixture.file(
        DOWNLOAD,
        Bytes::from(fixture!("internet_archive", "attachment.bin").await),
    );
    let service = authenticated_service(&fixture);
    assert!(!format!("{service:?}").contains("fixture-secret"));
    let item = service.item(ITEM).await.unwrap();
    assert!(!format!("{item:?}").contains("fixture-secret"));
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
    assert!(requests.iter().any(|r| r.method == http::Method::HEAD));
    assert!(requests.iter().any(|r| r.headers.contains_key("range")));
    for request in requests {
        assert_eq!(
            request.headers["authorization"],
            "LOW fixture-access:fixture-secret"
        );
        assert!(!request.path.contains("fixture-secret"));
        assert!(!request.path.contains("fixture-access"));
    }
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

#[tokio::test]
async fn preserves_file_names_without_path_validation() {
    let http = HttpFixture::new();
    http.respond(
        METADATA,
        StatusCode::OK,
        fixture!("internet_archive", "traversal.json").await,
    );
    let item = InternetArchiveService::with_endpoint(http.endpoint())
        .unwrap()
        .item(ITEM)
        .await
        .unwrap();
    assert_eq!(item.files()[0].name, "papers/../secret");
    assert_eq!(http.requests().len(), 1);
}

fn authenticated_service(http: &HttpFixture) -> InternetArchiveService {
    InternetArchiveService::with_endpoint(http.endpoint())
        .unwrap()
        .with_credentials("fixture-access", "fixture-secret")
        .unwrap()
        .with_context(opendal::OperationContext::new())
}

#[test]
fn rejects_invalid_credentials_before_io_without_exposing_them() {
    for (access, secret) in [
        ("", "secret"),
        ("access", ""),
        (" ", "secret"),
        ("access", "secret\r\nInjected: yes"),
        ("access:other", "secret"),
        ("access", "secret\0"),
    ] {
        let error = InternetArchiveService::new()
            .unwrap()
            .with_credentials(access, secret)
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(!format!("{error:?}").contains("Injected"));
    }
}

#[tokio::test]
async fn authenticated_requests_preserve_server_errors() {
    let fixture = HttpFixture::new();
    let service = authenticated_service(&fixture);
    for (status, kind) in [
        (StatusCode::UNAUTHORIZED, ErrorKind::Unauthenticated),
        (StatusCode::FORBIDDEN, ErrorKind::PermissionDenied),
    ] {
        fixture.respond(METADATA, status, Vec::new());
        assert_eq!(service.item(ITEM).await.unwrap_err().kind(), kind);
    }
    fixture.respond(
        METADATA,
        StatusCode::OK,
        fixture!("internet_archive", "item.json").await,
    );
    fixture.respond_for(
        http::Method::HEAD,
        DOWNLOAD,
        StatusCode::FORBIDDEN,
        Vec::new(),
    );
    let item = service.item(ITEM).await.unwrap();
    assert_eq!(
        item.open(NAME.into()).await.err().unwrap().kind(),
        ErrorKind::PermissionDenied
    );
}

#[tokio::test]
async fn retains_credentials_on_same_origin_and_strips_them_on_cross_origin_redirects() {
    let fixture = HttpFixture::new();
    let external = HttpFixture::new();
    fixture.respond(
        METADATA,
        StatusCode::OK,
        fixture!("internet_archive", "item.json").await,
    );
    let origin = fixture.endpoint().to_owned();
    let destination = external.endpoint().to_owned();
    fixture.handle(move |request| {
        let location = if request.uri().path() == "/local" {
            format!("{destination}/file")
        } else {
            format!("{origin}/local")
        };
        let mut redirect = response(StatusCode::TEMPORARY_REDIRECT, "");
        redirect
            .headers_mut()
            .insert("location", location.parse().unwrap());
        Ok(redirect)
    });
    external.file("/file", b"%PDFdata".as_slice());
    let item = authenticated_service(&fixture).item(ITEM).await.unwrap();
    let mut reader = item.open(NAME.into()).await.unwrap();
    assert_eq!(reader.read_range(1..4).await.unwrap().as_ref(), b"PDF");
    assert_eq!(read_all(reader, "test").await.unwrap(), b"%PDFdata");
    assert!(fixture.requests().iter().any(|r| r.path == "/local"));
    assert!(
        fixture
            .requests()
            .iter()
            .all(|r| r.headers["authorization"] == "LOW fixture-access:fixture-secret")
    );
    assert!(!external.requests().is_empty());
    assert!(
        external
            .requests()
            .iter()
            .all(|r| !r.headers.contains_key("authorization"))
    );
}

mod providers;
use http::StatusCode;
use open_arrow_access_core::{Bytes, ErrorKind};
use open_arrow_access_service::uspto::UsptoService;
use open_arrow_access_test_support::{byte_accessor, fixture, http::HttpFixture};

#[tokio::test]
async fn product_file_page_keeps_release_metadata_and_scopes_authentication() {
    let f = HttpFixture::new();
    let download = HttpFixture::new();
    let raw = String::from_utf8(fixture!("uspto", "product.json").await)
        .unwrap()
        .replace("ORIGIN", download.endpoint());
    f.respond(
        "/api/v1/datasets/products/PTGRXML?includeFiles=true&offset=0&limit=100",
        StatusCode::OK,
        raw.into_bytes(),
    );
    download.file("/grant.zip?sig=a%2Bb", Bytes::from_static(b"original zip"));
    let s = UsptoService::with_connection(
        providers::connection(&f)
            .with_header("X-API-KEY", "test")
            .unwrap(),
    );
    let files = s.product("PTGRXML", 0, 100).await.unwrap();
    assert_eq!(
        files.metadata().value()["bulkDataProductBag"][0]["productFileTotalQuantity"],
        1
    );
    assert_eq!(f.requests()[0].headers["x-api-key"], "test");
    assert!(download.requests().is_empty());
    byte_accessor::assert_read_contract(files, "grant.zip".into(), b"original zip")
        .await
        .unwrap();
    assert!(!download.requests()[0].headers.contains_key("x-api-key"));
}

#[tokio::test]
async fn rejects_wrong_product_and_missing_file_membership() {
    let f = HttpFixture::new();
    let s = UsptoService::with_connection(providers::connection(&f));
    for raw in [
        b"{\"bulkDataProductBag\":[]}".as_slice(),
        b"{\"bulkDataProductBag\":[{\"productIdentifier\":\"OTHER\"}]}",
        b"{\"bulkDataProductBag\":[{\"productIdentifier\":\"PTGRXML\"}]}",
    ] {
        f.respond(
            "/api/v1/datasets/products/PTGRXML?includeFiles=true&offset=0&limit=1",
            StatusCode::OK,
            Bytes::copy_from_slice(raw),
        );
        assert_eq!(
            s.product("PTGRXML", 0, 1).await.unwrap_err().kind(),
            ErrorKind::Decode
        );
    }
    assert!(UsptoService::new("").is_err());
    assert!(s.product("PTGRXML", 0, 0).await.is_err());
}

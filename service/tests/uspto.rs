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
            .with_header("x-client", "retained")
            .unwrap(),
    )
    .with_key("old-key")
    .unwrap()
    .with_key("test")
    .unwrap();
    assert!(!format!("{s:?}").contains("old-key"));
    let files = s.product("PTGRXML", 0, 100).await.unwrap();
    assert_eq!(
        files.metadata().value()["bulkDataProductBag"][0]["productFileTotalQuantity"],
        1
    );
    assert_eq!(f.requests()[0].headers["x-api-key"], "test");
    for request in f.requests() {
        assert_eq!(request.headers["x-api-key"], "test");
        assert_eq!(request.headers["x-client"], "retained");
        assert!(request.headers["x-api-key"].is_sensitive());
    }
    assert!(download.requests().is_empty());
    byte_accessor::assert_read_contract(files, "grant.zip".into(), b"original zip")
        .await
        .unwrap();
    assert!(!download.requests()[0].headers.contains_key("x-api-key"));
}

#[rstest::rstest]
#[case::empty_products("bad-empty-products.json")]
#[case::wrong_identity("bad-identity.json")]
#[case::missing_files("bad-missing-files.json")]
#[tokio::test]
async fn rejects_wrong_product_and_missing_file_membership(#[case] name: &str) {
    open_arrow_access_test_support::http::assert_resolution_cases(
        env!("CARGO_MANIFEST_DIR"),
        "uspto",
        "/api/v1/datasets/products/PTGRXML?includeFiles=true&offset=0&limit=1",
        &[(name, StatusCode::OK, ErrorKind::Decode)],
        |endpoint, context| async move {
            UsptoService::with_connection(providers::connection_at(&endpoint, context))
                .product("PTGRXML", 0, 1)
                .await
        },
    )
    .await;
}

#[tokio::test]
async fn invalid_key_and_page_size_are_rejected_before_io() {
    let f = HttpFixture::new();
    let s = UsptoService::with_connection(providers::connection(&f));
    assert!(UsptoService::new("").is_err());
    assert!(s.product("PTGRXML", 0, 0).await.is_err());
    assert!(f.requests().is_empty());
}

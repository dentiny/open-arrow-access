mod providers;
use http::StatusCode;
use open_arrow_access_core::{Bytes, ErrorKind};
use open_arrow_access_service::hathifiles::HathifilesService;
use open_arrow_access_test_support::{byte_accessor, fixture, http::HttpFixture};

#[tokio::test]
async fn catalog_keeps_updates_rights_and_original_gzip_bytes() {
    let f = HttpFixture::new();
    f.respond(
        "/files/hathifiles/hathi_file_list.json",
        StatusCode::OK,
        fixture!("hathifiles", "listing.json").await,
    );
    f.file(
        "/files/hathifiles/hathi_full_20261101.txt.gz",
        Bytes::from_static(b"gzip source"),
    );
    let s = HathifilesService::with_connection(providers::connection(&f));
    let files = s.catalog().await.unwrap();
    assert_eq!(files.metadata().value().as_array().unwrap().len(), 2);
    assert_eq!(files.metadata().value()[1]["full"], false);
    assert_eq!(f.requests().len(), 1);
    byte_accessor::assert_read_contract(files, "hathi_full_20261101.txt.gz".into(), b"gzip source")
        .await
        .unwrap();
}

#[tokio::test]
async fn missing_membership_fields_fail() {
    let f = HttpFixture::new();
    let s = HathifilesService::with_connection(providers::connection(&f));
    f.respond(
        "/files/hathifiles/hathi_file_list.json",
        StatusCode::OK,
        Bytes::from_static(b"{}"),
    );
    let error = s.catalog().await.unwrap_err();
    assert!(matches!(
        error.kind(),
        ErrorKind::Decode | ErrorKind::InvalidInput
    ));
}

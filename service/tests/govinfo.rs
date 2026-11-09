mod providers;
use http::StatusCode;
use open_arrow_access_core::{Bytes, ErrorKind};
use open_arrow_access_service::govinfo::GovInfoService;
use open_arrow_access_test_support::{byte_accessor, fixture, http::HttpFixture};
use providers::connection;

#[tokio::test]
async fn bulk_namespace_and_package_content_are_lazy() {
    let f = HttpFixture::new();
    f.respond(
        "/bulkdata/json/FR/2026",
        StatusCode::OK,
        fixture!("govinfo", "bulk.json").await,
    );
    f.file("/bulkdata/FR/2026/issue.xml", Bytes::from_static(b"<raw/>"));
    let s = GovInfoService::with_connections(
        connection(&f),
        connection(&f).with_query("api_key", "test"),
    );
    let files = s.bulk_directory("FR/2026").await.unwrap();
    assert_eq!(f.requests().len(), 1);
    assert_eq!(files.metadata().value()["files"][0]["folder"], true);
    byte_accessor::assert_read_contract(files, "issue.xml".into(), b"<raw/>")
        .await
        .unwrap();
    let raw = String::from_utf8(fixture!("govinfo", "package.json").await)
        .unwrap()
        .replace("ORIGIN", f.endpoint());
    f.respond(
        "/packages/P1/summary?api_key=test",
        StatusCode::OK,
        raw.into_bytes(),
    );
    f.file("/packages/P1/pdf?api_key=test", Bytes::from_static(b"%PDF"));
    let files = s.package("P1").await.unwrap();
    assert_eq!(files.metadata().value()["unknown"], "retained");
    byte_accessor::assert_read_contract(files, "pdfLink".into(), b"%PDF")
        .await
        .unwrap();
}

#[tokio::test]
async fn pages_validation_and_identity() {
    let f = HttpFixture::new();
    f.respond(
        "/collections/FR/2026-01-01?offsetMark=*&pageSize=10",
        StatusCode::OK,
        Bytes::from_static(b"{\"packages\":[],\"nextPage\":null}"),
    );
    let s = GovInfoService::with_connections(connection(&f), connection(&f));
    assert!(
        s.packages("FR", "2026-01-01", None, "*", 10)
            .await
            .unwrap()
            .value()["nextPage"]
            .is_null()
    );
    assert!(s.packages("FR", "x", None, "*", 1001).await.is_err());
    f.respond(
        "/packages/P1/summary",
        StatusCode::OK,
        Bytes::from_static(b"{\"packageId\":\"wrong\",\"download\":{}}"),
    );
    assert_eq!(s.package("P1").await.unwrap_err().kind(), ErrorKind::Decode);
    assert!(
        s.next_page("https://other.invalid/collections")
            .await
            .is_err()
    );
    assert!(s.bulk_directory("../FR").await.is_err());
}

use http::{Method, StatusCode};
use open_arrow_access_core::{Accessor, ByteReadRequest, Bytes, ErrorKind};
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::{collect_bytes, http::HttpFixture};

#[tokio::test]
async fn trusted_manifest_lengths_allow_get_only_downloads_and_keep_stat_fallback() {
    let fixture = HttpFixture::new();
    fixture.file("/original", Bytes::from_static(b"original bytes"));
    fixture.respond_for(
        Method::HEAD,
        "/original",
        StatusCode::FORBIDDEN,
        Bytes::new(),
    );
    fixture.file("/other", Bytes::from_static(b"other"));
    let operator =
        opendal::Operator::new(opendal_service_http::Http::default().endpoint(fixture.endpoint()))
            .unwrap();
    let source = OpenDalAccessor::new(operator);
    assert_eq!(
        source.open("original".into()).await.err().unwrap().kind(),
        ErrorKind::PermissionDenied
    );
    let source = source.with_content_lengths([("original".to_owned(), 14)]);
    let mut reader = source.open(ByteReadRequest::new("original")).await.unwrap();
    assert_eq!(reader.metadata().content_length, Some(14));
    assert_eq!(reader.read_range(0..8).await.unwrap().as_ref(), b"original");
    assert_eq!(
        collect_bytes(reader.as_mut()).await.unwrap(),
        b"original bytes"
    );
    assert_eq!(
        fixture
            .requests()
            .iter()
            .filter(|r| r.method == Method::HEAD && r.path == "/original")
            .count(),
        1
    );
    let mut other = source.open("other".into()).await.unwrap();
    assert_eq!(collect_bytes(other.as_mut()).await.unwrap(), b"other");
    assert!(
        fixture
            .requests()
            .iter()
            .any(|r| r.method == Method::HEAD && r.path == "/other")
    );
}

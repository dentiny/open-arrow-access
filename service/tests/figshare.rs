mod providers;
use http::StatusCode;
use open_arrow_access_core::{Bytes, ErrorKind};
use open_arrow_access_service::figshare::FigshareService;
use open_arrow_access_test_support::{byte_accessor, fixture, http::HttpFixture};

#[tokio::test]
async fn versioned_article_retains_licenses_and_lazily_reads_original_files() {
    let f = HttpFixture::new();
    let id = std::num::NonZeroU64::new(21422193).unwrap();
    let raw = String::from_utf8(fixture!("figshare", "article.json").await)
        .unwrap()
        .replace("ORIGIN", f.endpoint());
    f.respond(
        "/v2/articles/21422193/versions/4",
        StatusCode::OK,
        raw.into_bytes(),
    );
    f.file("/download?sig=a%2Bb", Bytes::from_static(b"OCR source"));
    let files = FigshareService::with_connection(providers::connection(&f))
        .article(id, std::num::NonZeroU32::new(4))
        .await
        .unwrap();
    assert_eq!(files.metadata().value()["license"]["name"], "CC0");
    assert_eq!(f.requests().len(), 1);
    byte_accessor::assert_read_contract(files, "ocr.txt.gz".into(), b"OCR source")
        .await
        .unwrap();
}

#[tokio::test]
async fn identity_embargo_and_missing_membership_fail() {
    let f = HttpFixture::new();
    let service = FigshareService::with_connection(providers::connection(&f));
    let id = std::num::NonZeroU64::new(1).unwrap();
    for (raw, kind) in [
        (b"{\"id\":2,\"files\":[]}".as_slice(), ErrorKind::Decode),
        (
            b"{\"id\":1,\"is_embargoed\":true,\"files\":[]}",
            ErrorKind::PermissionDenied,
        ),
        (b"{\"id\":1}", ErrorKind::Decode),
    ] {
        f.respond(
            "/v2/articles/1",
            StatusCode::OK,
            Bytes::copy_from_slice(raw),
        );
        assert_eq!(service.article(id, None).await.unwrap_err().kind(), kind);
    }
    assert!(service.collection_articles(id, 0, 10).await.is_err());
}

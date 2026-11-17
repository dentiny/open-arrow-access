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

#[rstest::rstest]
#[case::wrong_identity("bad-identity.json", ErrorKind::Decode)]
#[case::embargo("bad-embargo.json", ErrorKind::PermissionDenied)]
#[case::missing_files("bad-missing-files.json", ErrorKind::Decode)]
#[tokio::test]
async fn identity_embargo_and_missing_membership_fail(#[case] name: &str, #[case] kind: ErrorKind) {
    open_arrow_access_test_support::http::assert_resolution_cases(
        env!("CARGO_MANIFEST_DIR"),
        "figshare",
        "/v2/articles/1",
        &[(name, StatusCode::OK, kind)],
        |endpoint, context| async move {
            FigshareService::with_connection(providers::connection_at(&endpoint, context))
                .article(std::num::NonZeroU64::new(1).unwrap(), None)
                .await
        },
    )
    .await;
}

#[tokio::test]
async fn collection_page_zero_is_rejected_before_io() {
    let f = HttpFixture::new();
    let service = FigshareService::with_connection(providers::connection(&f));
    assert!(
        service
            .collection_articles(std::num::NonZeroU64::new(1).unwrap(), 0, 10)
            .await
            .is_err()
    );
    assert!(f.requests().is_empty());
}

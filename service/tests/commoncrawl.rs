use open_arrow_access_core::ErrorKind;
use open_arrow_access_service::commoncrawl::CommonCrawl;

#[test]
fn crawl_identity_and_mirror_urls_are_canonical() {
    let source =
        CommonCrawl::from_url("https://mirror.example/crawl-data/CC-MAIN-2022-40/").unwrap();
    assert_eq!(source.crawl(), "CC-MAIN-2022-40");
    assert_eq!(source.origin(), "https://mirror.example");
    assert_eq!(
        CommonCrawl::new("CC-MAIN-2022-40").unwrap().origin(),
        "https://data.commoncrawl.org"
    );
    let origin = "https://mirror.example/nested/";
    for (error, operation) in [
        (
            CommonCrawl::new("CC-MAIN-2022-40")
                .unwrap()
                .with_origin(origin)
                .unwrap_err(),
            "configure_commoncrawl",
        ),
        (
            open_arrow_access_service::commoncrawl::create_http_accessor(origin).unwrap_err(),
            "configure_commoncrawl_http",
        ),
    ] {
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.operation(), operation);
    }
}

#[rstest::rstest]
#[case::https("https://mirror.example")]
#[case::http_with_port("http://mirror.example:8080/")]
fn mirror_origins_are_canonical(#[case] origin: &str) {
    let source = CommonCrawl::new("CC-MAIN-2022-40")
        .unwrap()
        .with_origin(origin)
        .unwrap();
    assert_eq!(source.origin(), origin.trim_end_matches('/'));
    open_arrow_access_service::commoncrawl::create_http_accessor(origin).unwrap();
}

#[rstest::rstest]
#[case("../CC-MAIN-2022-40")]
#[case("CC-MAIN-22-40")]
#[case("CC-MAIN-2022-400")]
#[case("")]
#[case("cc-main-2022-40")]
#[case("CC-MAIN-202-40")]
#[case("CC-MAIN-2022-4")]
#[case("CC-MAIN-202x-40")]
#[case("CC-MAIN-2022-4x")]
#[case("CC-MAIN-2022-40-extra")]
#[case("CC-MAIN-2022-40 ")]
fn rejects_invalid_crawl_identities(#[case] crawl: &str) {
    let error = CommonCrawl::new(crawl).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput, "{crawl:?}");
    assert_eq!(error.operation(), "configure_commoncrawl");
}

#[rstest::rstest]
#[case("https://host/crawl-data/CC-MAIN-2022-40/?x=1")]
#[case("https://user:pass@host/crawl-data/CC-MAIN-2022-40/")]
#[case("https://host/x/../crawl-data/CC-MAIN-2022-40/")]
#[case("https://host/crawl-data/%43C-MAIN-2022-40/")]
#[case("https://host/crawl-data//CC-MAIN-2022-40/")]
#[case("https://host/crawl-data/CC-MAIN-2022-40//")]
#[case("file:///crawl-data/CC-MAIN-2022-40/")]
fn rejects_noncanonical_crawl_urls(#[case] value: &str) {
    assert_eq!(
        CommonCrawl::from_url(value).unwrap_err().kind(),
        ErrorKind::InvalidInput,
        "{value}"
    );
}

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
    for origin in ["https://mirror.example", "http://mirror.example:8080/"] {
        let source = CommonCrawl::new("CC-MAIN-2022-40")
            .unwrap()
            .with_origin(origin)
            .unwrap();
        assert_eq!(source.origin(), origin.trim_end_matches('/'));
        open_arrow_access_service::commoncrawl::create_http_accessor(origin).unwrap();
    }
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
    for crawl in ["../CC-MAIN-2022-40", "CC-MAIN-22-40", "CC-MAIN-2022-400"] {
        assert_eq!(
            CommonCrawl::new(crawl).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
    for value in [
        "https://host/crawl-data/CC-MAIN-2022-40/?x=1",
        "https://user:pass@host/crawl-data/CC-MAIN-2022-40/",
        "https://host/x/../crawl-data/CC-MAIN-2022-40/",
        "https://host/crawl-data/%43C-MAIN-2022-40/",
        "https://host/crawl-data//CC-MAIN-2022-40/",
        "https://host/crawl-data/CC-MAIN-2022-40//",
        "file:///crawl-data/CC-MAIN-2022-40/",
    ] {
        assert_eq!(
            CommonCrawl::from_url(value).unwrap_err().kind(),
            ErrorKind::InvalidInput,
            "{value}"
        );
    }
}

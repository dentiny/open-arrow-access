use super::validate_crawl;
use open_arrow_access_core::ErrorKind;

#[test]
fn validates_crawl_id_shape() {
    assert!(validate_crawl("CC-MAIN-2022-40").is_ok());
    for crawl in [
        "",
        "cc-main-2022-40",
        "CC-MAIN-202-40",
        "CC-MAIN-2022-4",
        "CC-MAIN-202x-40",
        "CC-MAIN-2022-4x",
        "CC-MAIN-2022-40-extra",
        "CC-MAIN-2022-40 ",
    ] {
        let error = validate_crawl(crawl).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "{crawl:?}");
        assert_eq!(error.operation(), "configure_commoncrawl");
    }
}

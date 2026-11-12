use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn invalid_url() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "configure_commoncrawl",
        "expected a canonical HTTP(S) URL without credentials, query or fragment",
    )
}

pub(super) fn invalid_crawl() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "configure_commoncrawl",
        "expected CC-MAIN-YYYY-NN",
    )
}

pub(super) fn invalid_origin_path(operation: &'static str) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        operation,
        "expected an HTTP(S) origin without a path",
    )
}

pub(super) fn invalid_crawl_path() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "configure_commoncrawl",
        "expected /crawl-data/<crawl>/",
    )
}

pub(super) fn http_configuration(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "configure_commoncrawl_http",
        "invalid HTTP configuration",
    )
    .with_source(error)
}

pub(super) fn url_parse(source: url::ParseError) -> Error {
    invalid_url().with_source(source)
}

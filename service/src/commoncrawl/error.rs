use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn invalid_url() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "configure_commoncrawl",
        "expected a canonical HTTP(S) URL without credentials, query or fragment",
    )
}

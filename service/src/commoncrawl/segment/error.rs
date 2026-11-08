use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn invalid_segment(message: &str) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "select_commoncrawl_segment",
        message,
    )
}

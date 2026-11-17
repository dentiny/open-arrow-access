use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn decode(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Decode, "read_pmc", message)
}

pub(super) fn invalid_selection() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "discover_pmc",
        "expected a bucket-relative metadata .json object or prefix ending in /",
    )
}

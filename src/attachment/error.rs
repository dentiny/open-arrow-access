use crate::{Error, ErrorKind};

pub(super) fn invalid(message: &str) -> Error {
    Error::new(ErrorKind::InvalidInput, "open_attachment_source", message)
}

pub(super) fn missing_format(feature: &str) -> Error {
    Error::new(
        ErrorKind::Unsupported,
        "open_attachment_source",
        format!("attachment decoding requires Cargo feature '{feature}'"),
    )
}

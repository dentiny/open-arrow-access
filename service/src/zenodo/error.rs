use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn invalid(message: &str) -> Error {
    Error::new(ErrorKind::InvalidInput, "configure_zenodo", message)
}
pub(super) fn decode(message: &str) -> Error {
    Error::new(ErrorKind::Decode, "resolve_zenodo", message)
}
pub(super) fn io_error(error: opendal::Error) -> Error {
    let kind = match error.kind() {
        opendal::ErrorKind::NotFound => ErrorKind::NotFound,
        opendal::ErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
        _ => ErrorKind::Io,
    };
    Error::new(kind, "resolve_zenodo", "Zenodo HTTP operation failed").with_source(error)
}

pub(super) fn membership_error(error: Error) -> Error {
    decode("invalid attachment membership").with_source(error)
}

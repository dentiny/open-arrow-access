use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn invalid(message: &str) -> Error {
    Error::new(ErrorKind::InvalidInput, super::CONFIGURE, message)
}
pub(super) fn decode(message: &str) -> Error {
    Error::new(ErrorKind::Decode, super::RESOLVE, message)
}
pub(super) fn not_found() -> Error {
    Error::new(
        ErrorKind::NotFound,
        super::RESOLVE,
        "Internet Archive item not found",
    )
}
pub(super) fn unavailable() -> Error {
    Error::new(
        ErrorKind::PermissionDenied,
        super::RESOLVE,
        "Internet Archive item is not publicly available",
    )
}
pub(super) fn membership_error(error: Error) -> Error {
    decode("invalid item file membership").with_source(error)
}

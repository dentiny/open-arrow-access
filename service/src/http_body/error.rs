use open_arrow_access_core::{Error, ErrorKind};
pub(super) fn body_error(operation: &'static str, message: &str) -> Error {
    Error::new(ErrorKind::Decode, operation, message)
}

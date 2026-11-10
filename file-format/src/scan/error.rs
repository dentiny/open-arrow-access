use open_arrow_access_core::{Error, ErrorKind};

pub(crate) fn invalid_frame(operation: &'static str, message: &'static str) -> Error {
    Error::new(ErrorKind::Decode, operation, message)
}

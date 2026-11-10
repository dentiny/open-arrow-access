use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn invalid_frame(operation: &'static str, message: &'static str) -> Error {
    Error::new(ErrorKind::Decode, operation, message)
}

pub(super) fn decode_error(error: arrow_schema::ArrowError) -> Error {
    Error::new(ErrorKind::Decode, "decode_csv", error.to_string()).with_source(error)
}

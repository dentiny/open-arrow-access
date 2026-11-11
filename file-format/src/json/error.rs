use arrow_schema::ArrowError;
use open_arrow_access_core::{Error, ErrorKind};

pub(crate) fn invalid_frame(operation: &'static str, message: &'static str) -> Error {
    Error::new(ErrorKind::Decode, operation, message)
}

pub(crate) fn json_frame(source: jiter::JiterError) -> Error {
    Error::new(ErrorKind::Decode, "frame_json", source.to_string()).with_source(source)
}

pub(super) fn decode_error(error: ArrowError) -> Error {
    Error::new(ErrorKind::Decode, "decode_json", error.to_string()).with_source(error)
}

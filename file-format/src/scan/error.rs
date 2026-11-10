use open_arrow_access_core::{Error, ErrorKind};

pub(crate) fn invalid_frame(operation: &'static str, message: &'static str) -> Error {
    Error::new(ErrorKind::Decode, operation, message)
}

#[cfg(feature = "json")]
pub(crate) fn json_frame(source: serde_json::Error) -> Error {
    Error::new(ErrorKind::Decode, "frame_json", source.to_string()).with_source(source)
}

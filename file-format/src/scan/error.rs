use open_arrow_access_core::{Error, ErrorKind};

pub(crate) fn record_too_large(operation: &'static str) -> Error {
    Error::new(
        ErrorKind::ResourceExhausted,
        operation,
        "serialized record exceeds max_record_bytes",
    )
}

pub(crate) fn invalid_frame(operation: &'static str, message: &'static str) -> Error {
    Error::new(ErrorKind::Decode, operation, message)
}

use open_arrow_access_core::{Error, ErrorKind};
pub(super) fn body_error(operation: &'static str, message: &str) -> Error {
    Error::new(ErrorKind::Decode, operation, message)
}

pub(super) fn body_limit_exceeded(operation: &'static str, limit: Option<usize>) -> Error {
    Error::new(
        ErrorKind::ResourceExhausted,
        operation,
        match limit {
            Some(limit) => format!("metadata response exceeds {limit} byte limit"),
            None => "collected body length overflow".into(),
        },
    )
}

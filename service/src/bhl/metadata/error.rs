use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidInput, "bhl_metadata", message)
}

pub(super) fn json(source: serde_json::Error) -> Error {
    Error::new(ErrorKind::Decode, "bhl_metadata", source.to_string()).with_source(source)
}

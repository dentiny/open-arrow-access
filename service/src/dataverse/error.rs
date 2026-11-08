use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn invalid(message: &str) -> Error {
    Error::new(ErrorKind::InvalidInput, "configure_dataverse", message)
}
pub(super) fn decode(message: &str) -> Error {
    Error::new(ErrorKind::Decode, "resolve_dataverse", message)
}
pub(super) fn map_file_namespace_error(error: Error) -> Error {
    decode("invalid or duplicate file membership").with_source(error)
}

pub(super) fn unsupported(message: &str) -> Error {
    Error::new(ErrorKind::Unsupported, "resolve_dataverse", message)
}

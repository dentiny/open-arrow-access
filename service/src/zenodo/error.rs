use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn decode(message: &str) -> Error {
    Error::new(ErrorKind::Decode, "resolve_zenodo", message)
}

pub(super) fn map_file_namespace_error(error: Error) -> Error {
    decode("invalid attachment membership").with_source(error)
}

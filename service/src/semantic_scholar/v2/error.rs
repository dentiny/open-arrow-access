use arrow_schema::ArrowError;
use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn arrow_error(source: ArrowError) -> Error {
    error(source.to_string()).with_source(source)
}
pub(super) fn error(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Decode, "decode_s2orc_v2", message)
}

pub(super) fn annotation_error(name: &str, cause: Error) -> Error {
    error(format!("{name}: {cause}"))
}

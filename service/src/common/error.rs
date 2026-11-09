use open_arrow_access_core::{Error, ErrorKind};

pub(super) const OP: &str = "provider_http";

pub(crate) fn invalid(message: &str) -> Error {
    Error::new(ErrorKind::InvalidInput, OP, message)
}
pub(crate) fn decode(message: &str) -> Error {
    Error::new(ErrorKind::Decode, OP, message)
}
pub(super) fn download_url(error: url::ParseError) -> Error {
    decode("invalid download URL").with_source(error)
}
pub(super) fn missing_field(field: &str) -> Error {
    decode(&format!("missing or invalid {field}"))
}

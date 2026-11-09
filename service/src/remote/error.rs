use open_arrow_access_core::{Error, ErrorKind};

pub(super) const OP: &str = "provider_http";

pub(crate) fn invalid(message: &str) -> Error {
    Error::new(ErrorKind::InvalidInput, OP, message)
}
pub(crate) fn decode(message: &str) -> Error {
    Error::new(ErrorKind::Decode, OP, message)
}
pub(super) fn io(error: opendal::Error) -> Error {
    crate::http_client::transport_error(OP, error)
}

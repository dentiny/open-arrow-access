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

pub(super) fn request(error: http::Error) -> Error {
    crate::http_client::request_error(OP, error)
}

pub(super) fn json(error: serde_json::Error) -> Error {
    crate::http_client::json_error(OP, error)
}

pub(super) fn download_url(error: url::ParseError) -> Error {
    decode("invalid download URL").with_source(error)
}

pub(super) fn provider_path(error: url::ParseError) -> Error {
    invalid("invalid provider path").with_source(error)
}

pub(super) fn redirect(error: url::ParseError) -> Error {
    decode("invalid redirect").with_source(error)
}

pub(super) fn header_name(error: http::header::InvalidHeaderName) -> Error {
    invalid("invalid header name").with_source(error)
}

pub(super) fn header_value(error: http::header::InvalidHeaderValue) -> Error {
    invalid("invalid header value").with_source(error)
}

pub(super) fn missing_field(field: &str) -> Error {
    decode(&format!("missing or invalid {field}"))
}

pub(super) fn file_namespace(error: Error) -> Error {
    decode("invalid file namespace").with_source(error)
}

pub(super) fn http(status: http::StatusCode) -> Error {
    Error::new(
        ErrorKind::from_http_status(status.as_u16()),
        OP,
        format!("provider HTTP {status}"),
    )
}

pub(super) fn transport(error: Error) -> opendal::Error {
    opendal::Error::new(opendal::ErrorKind::Unexpected, "provider redirect failed")
        .set_source(error)
}

pub(super) fn transport_request(error: http::Error) -> opendal::Error {
    transport(request(error))
}

pub(super) fn missing_auth_scope() -> opendal::Error {
    opendal::Error::new(
        opendal::ErrorKind::ConfigInvalid,
        "missing provider authentication scope",
    )
}

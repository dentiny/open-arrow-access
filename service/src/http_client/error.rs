use open_arrow_access_core::{Error, ErrorKind};

pub(crate) fn transport_error(operation: &'static str, error: opendal::Error) -> Error {
    let kind = ErrorKind::from_source(&error)
        .unwrap_or_else(|| crate::opendal_error::error_kind(error.kind()));
    Error::new(kind, operation, "provider HTTP operation failed").with_source(error)
}

pub(super) fn invalid_origin(operation: &'static str) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        operation,
        "expected an HTTP(S) origin without credentials, query or fragment",
    )
}

pub(super) fn invalid_download_url(
    error: impl std::error::Error + Send + Sync + 'static,
) -> opendal::Error {
    opendal::Error::new(
        opendal::ErrorKind::ConfigInvalid,
        "invalid HTTP download URL",
    )
    .set_source(error)
}

pub(crate) fn client_error(error: reqwest::Error) -> Error {
    Error::new(
        ErrorKind::Io,
        "configure_http",
        "failed to build HTTP client",
    )
    .with_source(error)
}
pub(crate) fn request_error(operation: &'static str, error: http::Error) -> Error {
    Error::new(ErrorKind::InvalidInput, operation, "invalid HTTP request").with_source(error)
}
pub(crate) fn json_error(operation: &'static str, error: serde_json::Error) -> Error {
    Error::new(ErrorKind::Decode, operation, "invalid JSON metadata").with_source(error)
}

/// Translate metadata response failures directly into the shared core category.
pub(super) fn http_status_error(operation: &'static str, status: http::StatusCode) -> Error {
    Error::new(
        ErrorKind::from_http_status(status.as_u16()),
        operation,
        format!("metadata HTTP {status}"),
    )
}

pub(super) fn file_namespace() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "configure_resolved_files",
        "empty or duplicate filename",
    )
}

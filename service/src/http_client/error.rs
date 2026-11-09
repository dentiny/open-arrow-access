use http::StatusCode;
use open_arrow_access_core::{Error, ErrorKind, Result};

pub(crate) fn transport_error(operation: &'static str, error: opendal::Error) -> Error {
    let kind = crate::opendal_error::error_kind(error.kind());
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

pub(crate) fn check_status(
    status: StatusCode,
    headers: &http::HeaderMap,
    operation: &'static str,
) -> Result<()> {
    if status == StatusCode::OK {
        return Ok(());
    }
    let kind = match status {
        StatusCode::NOT_FOUND => opendal::ErrorKind::NotFound,
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => opendal::ErrorKind::PermissionDenied,
        StatusCode::TOO_MANY_REQUESTS => opendal::ErrorKind::RateLimited,
        _ => opendal::ErrorKind::Unexpected,
    };
    Err(Error::new(
        crate::opendal_error::error_kind(kind),
        operation,
        format!("provider HTTP {status}"),
    )
    .with_source(HttpStatusError {
        status,
        retry_after: headers
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned),
    }))
}

/// Status error retained in the shared Error source chain. No URL or credential is stored.
#[derive(Debug)]
pub struct HttpStatusError {
    pub status: StatusCode,
    /// Upstream Retry-After value (seconds or HTTP date), unchanged.
    pub retry_after: Option<String>,
}
impl std::fmt::Display for HttpStatusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HTTP {}", self.status)
    }
}
impl std::error::Error for HttpStatusError {}

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

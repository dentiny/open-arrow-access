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
pub(crate) fn http_status_error(
    operation: &'static str,
    status: http::StatusCode,
    headers: &http::HeaderMap,
) -> Error {
    Error::new(
        ErrorKind::from_http_status(status.as_u16()),
        operation,
        format!("HTTP {status}"),
    )
    .with_http_response(
        status.as_u16(),
        headers
            .get(http::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
    )
}

pub(super) fn empty_or_dup_filename() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "configure_resolved_files",
        "empty or duplicate filename",
    )
}

// Keep failed responses intact so exhausted retries retain HTTP metadata.
pub(super) enum Failure {
    Transport(opendal::Error),
    Response(Box<http::Response<opendal::HttpBody>>),
}

impl Failure {
    pub(super) fn retry_after(&self) -> Option<std::time::Duration> {
        match self {
            Self::Response(response) => {
                super::validation::retry_after(response.headers(), std::time::SystemTime::now())
            }
            Self::Transport(_) => None,
        }
    }

    pub(super) fn is_retryable(&self) -> bool {
        match self {
            Self::Transport(error) => error.is_temporary(),
            Self::Response(_) => true,
        }
    }
}

pub(super) fn classify_response(
    result: opendal::Result<http::Response<opendal::HttpBody>>,
) -> std::result::Result<http::Response<opendal::HttpBody>, Failure> {
    match result {
        Ok(response)
            if matches!(
                response.status().as_u16(),
                408 | 429 | 500 | 502 | 503 | 504
            ) =>
        {
            Err(Failure::Response(Box::new(response)))
        }
        Ok(response) => Ok(response),
        Err(error) => Err(Failure::Transport(error)),
    }
}

pub(super) fn timeout_error() -> opendal::Error {
    opendal::Error::new(opendal::ErrorKind::Unexpected, "provider HTTP timeout")
        .set_source(Error::new(
            ErrorKind::Timeout,
            "provider_http",
            "provider HTTP timeout",
        ))
        .set_temporary()
}

pub(super) fn invalid_token(
    operation: &'static str,
    source: http::header::InvalidHeaderValue,
) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        operation,
        "invalid HTTP authentication token",
    )
    .with_source(source)
}

pub(super) fn invalid_content_length(operation: &'static str) -> Error {
    Error::new(ErrorKind::Decode, operation, "invalid Content-Length")
}

pub(super) fn body_length_overflow(operation: &'static str) -> Error {
    Error::new(ErrorKind::Decode, operation, "body length overflow")
}

pub(super) fn excessive_body_length(operation: &'static str) -> Error {
    Error::new(
        ErrorKind::Decode,
        operation,
        "HTTP body exceeds Content-Length",
    )
}

pub(super) fn truncated_body(operation: &'static str) -> Error {
    Error::new(ErrorKind::Decode, operation, "truncated HTTP body")
}

pub(super) fn finish_response(
    result: std::result::Result<http::Response<opendal::HttpBody>, Failure>,
) -> opendal::Result<http::Response<opendal::HttpBody>> {
    match result {
        Ok(response) => Ok(response),
        Err(Failure::Response(response)) => Ok(*response),
        Err(Failure::Transport(error)) => Err(error),
    }
}

use std::{error::Error as StdError, io, sync::Arc};

/// The result type shared by all components.
pub type Result<T> = std::result::Result<T, Error>;

/// Backend-independent error categories. Original errors remain in the source chain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ErrorKind {
    InvalidInput,
    Unsupported,
    NotFound,
    /// Credentials are missing, invalid, or required by a proxy or network.
    Unauthenticated,
    PermissionDenied,
    /// The operation conflicts with the current resource state, including a lock.
    Conflict,
    /// A required condition is missing or evaluates to false.
    ConditionNotMet,
    Timeout,
    RateLimited,
    /// The endpoint or an upstream dependency cannot currently accept the request.
    Unavailable,
    /// A storage or other capacity limit prevents the operation.
    ResourceExhausted,
    Io,
    Decode,
}

impl ErrorKind {
    /// Classify HTTP failures without implying that a retry is safe.
    /// Unknown 4xx statuses (including unused 418) use `InvalidInput`;
    /// unknown 5xx, successful, redirect and invalid statuses use `Io`.
    /// See <https://www.iana.org/assignments/http-status-codes/>.
    pub fn from_http_status(status: u16) -> Self {
        match status {
            400 | 411 | 413 | 414 | 416 | 417 | 422 | 431 => Self::InvalidInput,
            401 | 407 | 511 => Self::Unauthenticated,
            402 | 403 | 451 => Self::PermissionDenied,
            404 | 410 => Self::NotFound,
            405 | 406 | 415 | 426 | 501 | 505 | 510 => Self::Unsupported,
            408 | 504 => Self::Timeout,
            409 | 423 => Self::Conflict,
            412 | 428 => Self::ConditionNotMet,
            421 | 425 | 502 | 503 => Self::Unavailable,
            429 => Self::RateLimited,
            507 => Self::ResourceExhausted,
            424 | 500 | 506 | 508 => Self::Io,
            400..=499 => Self::InvalidInput,
            _ => Self::Io,
        }
    }

    /// Find the first shared error category in a source chain, including boxed and shared I/O errors.
    pub fn from_source(mut source: &(dyn StdError + 'static)) -> Option<Self> {
        loop {
            if let Some(error) = source.downcast_ref::<Error>() {
                return Some(error.kind());
            }
            if let Some(shared) = source.downcast_ref::<Arc<io::Error>>() {
                source = shared.as_ref();
                continue;
            }
            source = match source
                .downcast_ref::<io::Error>()
                .and_then(io::Error::get_ref)
            {
                Some(inner) => inner,
                None => source.source()?,
            };
        }
    }
}

/// An error with operation context and an optional original error.
#[derive(Debug, thiserror::Error)]
#[error("{operation}: {message}")]
pub struct Error {
    kind: ErrorKind,
    operation: &'static str,
    message: String,
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
    http_status: Option<u16>,
    retry_after: Option<String>,
}

impl Error {
    pub fn new(kind: ErrorKind, operation: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind,
            operation,
            message: message.into(),
            source: None,
            http_status: None,
            retry_after: None,
        }
    }

    pub fn with_source<E>(mut self, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        self.source = Some(Box::new(source));
        self
    }

    /// Retain HTTP failure metadata without imposing a retry policy.
    pub fn with_http_response(mut self, status: u16, retry_after: Option<String>) -> Self {
        self.http_status = Some(status);
        self.retry_after = retry_after;
        self
    }

    /// Original HTTP response status, when this error came from a response.
    pub fn http_status(&self) -> Option<u16> {
        self.http_status
    }

    /// Original Retry-After header: delta-seconds or an HTTP-date. The caller
    /// interprets the value and decides whether and when to retry. Invalid values
    /// are retained; this library does not silently replace them with a delay.
    pub fn retry_after(&self) -> Option<&str> {
        self.retry_after.as_deref()
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub fn operation(&self) -> &'static str {
        self.operation
    }
}

pub(crate) fn invalid_output_schema() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "build_record_batch",
        "output schema must contain unique fields from the full schema",
    )
}

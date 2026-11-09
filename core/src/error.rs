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
    /// Classify HTTP failures using backend-independent error categories.
    ///
    /// Covers registered 4xx/5xx statuses, including WebDAV and proxy failures.
    /// Unknown 4xx codes follow the generic 400 category (`InvalidInput`);
    /// unknown 5xx codes use `Io`. Unused 418 also follows the 4xx fallback.
    ///
    /// Callers handle successful responses and redirects before calling this method.
    /// Non-error or invalid status codes defensively return `Io`.
    /// Categories alone do not imply that an operation is safe to retry.
    ///
    /// Status definitions: <https://www.iana.org/assignments/http-status-codes/>.
    pub fn from_http_status(status: u16) -> Self {
        match status {
            // Request syntax, content, length, range, or expectation is unacceptable.
            400 | 411 | 413 | 414 | 416 | 417 | 422 | 431 => Self::InvalidInput,
            401 | 407 | 511 => Self::Unauthenticated,
            402 | 403 | 451 => Self::PermissionDenied,
            404 | 410 => Self::NotFound,
            // Method, representation, protocol version, or extension is unsupported.
            405 | 406 | 415 | 426 | 501 | 505 | 510 => Self::Unsupported,
            408 | 504 => Self::Timeout,
            409 | 423 => Self::Conflict,
            412 | 428 => Self::ConditionNotMet,
            // Routing, early-data, gateway, or service availability failures.
            421 | 425 | 502 | 503 => Self::Unavailable,
            429 => Self::RateLimited,
            507 => Self::ResourceExhausted,
            // Failed dependency, internal/negotiation errors, and dependency loops.
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
}

impl Error {
    pub fn new(kind: ErrorKind, operation: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind,
            operation,
            message: message.into(),
            source: None,
        }
    }

    pub fn with_source<E>(mut self, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        self.source = Some(Box::new(source));
        self
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub fn operation(&self) -> &'static str {
        self.operation
    }
}

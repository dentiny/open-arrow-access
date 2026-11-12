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

pub(crate) fn scan_schema_mismatch() -> Error {
    Error::new(
        ErrorKind::Decode,
        "read_scan",
        "partition schema differs from scan schema",
    )
}

pub(crate) fn record_batch_decode(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Decode, "build_record_batch", message)
}

pub(crate) fn failed_builder() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "build_record_batch",
        "builder is invalid after an append error",
    )
}

pub(crate) fn invalid_byte_range(
    range: &std::ops::Range<u64>,
    len: u64,
    operation: &'static str,
) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        operation,
        format!(
            "range {}..{} is outside object length {}",
            range.start, range.end, len,
        ),
    )
}

pub(crate) fn duplicate_schema_field() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "build_record_batch",
        "duplicate schema field name",
    )
}

pub(crate) fn unsupported_metadata_type(data_type: &arrow_schema::DataType) -> Error {
    Error::new(
        ErrorKind::Unsupported,
        "build_record_batch",
        format!("unsupported metadata type {data_type}"),
    )
}

pub(crate) fn duplicate_projection_field(name: &str) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "project",
        format!("duplicate field {name:?}"),
    )
}

pub(crate) fn unknown_projection_field(
    name: &str,
    error: impl std::error::Error + Send + Sync + 'static,
) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "project",
        format!("unknown field {name:?}"),
    )
    .with_source(error)
}

pub(crate) fn projection_error(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::new(ErrorKind::InvalidInput, "project", error.to_string()).with_source(error)
}

pub(crate) fn decoder_empty_chunk(operation: &'static str, message: &'static str) -> Error {
    Error::new(ErrorKind::Io, operation, message)
}

pub(crate) fn decoder_overconsumed(operation: &'static str) -> Error {
    Error::new(
        ErrorKind::Decode,
        operation,
        "decoder consumed more bytes than supplied",
    )
}

pub(crate) fn decoder_no_progress(operation: &'static str) -> Error {
    Error::new(ErrorKind::Decode, operation, "decoder made no progress")
}

pub(crate) fn unknown_field(name: &str) -> arrow_schema::ArrowError {
    arrow_schema::ArrowError::SchemaError(format!("unknown field {name:?}"))
}

pub(crate) fn empty_chunk(operation: &'static str) -> Error {
    Error::new(
        ErrorKind::Io,
        operation,
        "byte reader returned an empty chunk",
    )
}

pub(crate) fn line_too_long(operation: &'static str, limit: usize) -> Error {
    Error::new(
        ErrorKind::Decode,
        operation,
        format!("line exceeds configured {limit}-byte limit"),
    )
}

pub(crate) fn reading_unsupported(operation: &'static str) -> Error {
    Error::new(
        ErrorKind::Unsupported,
        operation,
        "accessor does not support reading",
    )
}

pub(crate) fn listing_unsupported() -> Error {
    Error::new(
        ErrorKind::Unsupported,
        "list_files",
        "accessor does not support listing",
    )
}

pub(crate) fn listing_outside_prefix() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "list_files",
        "listing returned an object outside its directory prefix",
    )
}

pub(crate) fn invalid_document_batch() -> Error {
    Error::new(
        ErrorKind::Decode,
        "decode_document",
        "decoder must return one row with the projected schema",
    )
}

pub(crate) fn document_batch_error(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::new(ErrorKind::Decode, "decode_document", error.to_string()).with_source(error)
}

pub(crate) fn project_batch_error(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::new(ErrorKind::Decode, "project_batch", error.to_string()).with_source(error)
}

pub(crate) fn seek_requires_ranges(operation: &'static str) -> Error {
    Error::new(
        ErrorKind::Unsupported,
        operation,
        "range reads are required",
    )
}

pub(crate) fn seek_requires_length(operation: &'static str) -> Error {
    Error::new(
        ErrorKind::Unsupported,
        operation,
        "known input length is required",
    )
}

pub(crate) fn incorrect_range_length(operation: &'static str) -> Error {
    Error::new(
        ErrorKind::Io,
        operation,
        "range reader returned an incorrect length",
    )
}

pub(crate) fn invalid_seek() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid seek")
}

pub(crate) fn default_listing_unsupported() -> Error {
    Error::new(
        ErrorKind::Unsupported,
        "list",
        "accessor does not support discovery",
    )
}

pub(crate) fn default_range_unsupported() -> Error {
    Error::new(
        ErrorKind::Unsupported,
        "read_range",
        "reader is sequential only",
    )
}

pub(crate) fn record_batch_error(source: arrow_schema::ArrowError) -> Error {
    record_batch_decode(source.to_string()).with_source(source)
}

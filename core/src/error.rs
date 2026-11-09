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
    PermissionDenied,
    Io,
    Decode,
}

impl ErrorKind {
    /// Classify an HTTP failure without depending on a storage backend.
    /// Callers handle successful responses and redirects before translating failures.
    /// Rate limits and other HTTP failures retain the existing `Io` category.
    pub fn from_http_status(status: u16) -> Self {
        match status {
            404 => Self::NotFound,
            401 | 403 => Self::PermissionDenied,
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

use std::{error::Error as StdError, io};

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
    /// Find the first shared error category in a source chain, including boxed I/O errors.
    pub fn from_source(mut source: &(dyn StdError + 'static)) -> Option<Self> {
        loop {
            if let Some(error) = source.downcast_ref::<Error>() {
                return Some(error.kind());
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

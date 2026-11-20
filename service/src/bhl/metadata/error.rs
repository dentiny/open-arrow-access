use open_arrow_access_core::{Error as AccessError, ErrorKind};
#[derive(Debug, thiserror::Error)]
pub(super) enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Access(#[from] AccessError),
    #[error("{0}")]
    Invalid(String),
}
pub(super) type Result<T> = std::result::Result<T, Error>;
impl From<Error> for AccessError {
    fn from(source: Error) -> Self {
        let kind = match &source {
            Error::Access(source) => source.kind(),
            Error::Io(source) if source.kind() == std::io::ErrorKind::NotFound => {
                ErrorKind::NotFound
            }
            Error::Io(_) | Error::Sqlite(_) => ErrorKind::Io,
            Error::Json(_) => ErrorKind::Decode,
            Error::Invalid(_) => ErrorKind::InvalidInput,
        };
        AccessError::new(kind, "bhl_metadata", source.to_string()).with_source(source)
    }
}
pub(super) fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}

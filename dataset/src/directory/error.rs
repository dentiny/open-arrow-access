use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn glob_error(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "select_directory",
        error.to_string(),
    )
    .with_source(error)
}

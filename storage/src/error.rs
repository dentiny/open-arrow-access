use open_arrow_access_core::{Error, ErrorKind};

pub(crate) fn map_error(operation: &'static str, path: &str, error: opendal::Error) -> Error {
    let kind =
        ErrorKind::from_source(&error).unwrap_or_else(|| crate::opendal_error_kind(error.kind()));
    Error::new(kind, operation, format!("object {path:?}")).with_source(error)
}

pub(crate) fn invalid_names() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "configure_named_files",
        "empty or duplicate filename",
    )
}

pub(crate) fn invalid_retry_factor() -> Error {
    invalid_config("retry factor must be finite and at least 1")
}

pub(crate) fn invalid_retry_delays() -> Error {
    invalid_config("retry min_delay must not exceed max_delay")
}

pub(crate) fn invalid_timeouts() -> Error {
    invalid_config("storage timeouts must be positive")
}

fn invalid_config(message: &'static str) -> Error {
    Error::new(ErrorKind::InvalidInput, "configure_storage", message)
}

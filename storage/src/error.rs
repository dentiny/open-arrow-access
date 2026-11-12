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

pub(crate) fn short_read(operation: &'static str) -> Error {
    Error::new(
        ErrorKind::Io,
        operation,
        "unexpected short read; input may have changed",
    )
}

/// Classify OpenDAL I/O failures; unrecognized categories map to `Io`.
/// Callers handle configuration errors separately.
pub fn opendal_error_kind(kind: opendal::ErrorKind) -> open_arrow_access_core::ErrorKind {
    use open_arrow_access_core::ErrorKind;

    match kind {
        opendal::ErrorKind::NotFound => ErrorKind::NotFound,
        opendal::ErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
        opendal::ErrorKind::Unsupported => ErrorKind::Unsupported,
        _ => ErrorKind::Io,
    }
}

pub(crate) fn not_regular_file() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "open",
        "object is not a regular file",
    )
}

pub(crate) fn listing_unsupported() -> Error {
    Error::new(
        ErrorKind::Unsupported,
        "list",
        "operator does not support listing",
    )
}

pub(crate) fn input_too_long() -> Error {
    Error::new(ErrorKind::Io, "read", "input exceeded its declared length")
}

pub(crate) fn unaddressable_range() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "read_range",
        "range exceeds addressable memory",
    )
}

pub(crate) fn unknown_named_file() -> Error {
    Error::new(
        ErrorKind::NotFound,
        "open_named_file",
        "filename is absent from manifest",
    )
}

pub(crate) fn invalid_listing_prefix() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "list_named_files",
        "expected an empty root prefix",
    )
}

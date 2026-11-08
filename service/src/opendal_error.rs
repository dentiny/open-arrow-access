use open_arrow_access_core::ErrorKind;

/// Classify provider OpenDAL errors; callers retain operation and source context.
pub(crate) fn error_kind(kind: opendal::ErrorKind) -> ErrorKind {
    match kind {
        opendal::ErrorKind::ConfigInvalid => ErrorKind::InvalidInput,
        opendal::ErrorKind::NotFound => ErrorKind::NotFound,
        opendal::ErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
        opendal::ErrorKind::Unsupported => ErrorKind::Unsupported,
        _ => ErrorKind::Io,
    }
}

use open_arrow_access_core::ErrorKind;

/// Classify provider OpenDAL errors; callers retain operation and source context.
pub(crate) fn error_kind(error: &opendal::Error) -> ErrorKind {
    if error.kind() == opendal::ErrorKind::ConfigInvalid {
        ErrorKind::from_source(error).unwrap_or(ErrorKind::InvalidInput)
    } else {
        open_arrow_access_storage::classify_opendal_error(error)
    }
}

use open_arrow_access_core::ErrorKind;

/// Classify provider OpenDAL errors; callers retain operation and source context.
pub(crate) fn error_kind(kind: opendal::ErrorKind) -> ErrorKind {
    if kind == opendal::ErrorKind::ConfigInvalid {
        ErrorKind::InvalidInput
    } else {
        open_arrow_access_storage::opendal_error_kind(kind)
    }
}

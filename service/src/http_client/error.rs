use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn transport_error(operation: &'static str, error: opendal::Error) -> Error {
    let kind = match error.kind() {
        opendal::ErrorKind::ConfigInvalid => ErrorKind::InvalidInput,
        opendal::ErrorKind::NotFound => ErrorKind::NotFound,
        opendal::ErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
        opendal::ErrorKind::Unsupported => ErrorKind::Unsupported,
        _ => ErrorKind::Io,
    };
    Error::new(kind, operation, "provider HTTP operation failed").with_source(error)
}

//! Storage adapters use OpenDAL and expose only the shared read contracts.

mod backend;
mod validation;

pub use backend::OpenDalAccessor;

/// OpenDAL SDK used by the generic adapter.
pub use opendal;

mod named_files;
pub use named_files::{NamedFileAccessor, validate_file_names};

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

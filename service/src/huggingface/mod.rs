//! Hugging Face repository bytes and Parquet dataset access.

mod accessor;
mod client;
mod dataset;
mod metadata;
mod options;
mod request;

pub use accessor::HuggingFaceAccessor;
pub use client::HuggingFaceService;
pub use options::{HuggingFaceOptions, HuggingFaceRepository};
pub use request::HuggingFaceDatasetRequest;

use open_arrow_access_core::{Error, ErrorKind};

fn map_error(operation: &'static str, path: &str, error: opendal::Error) -> Error {
    let kind = match error.kind() {
        opendal::ErrorKind::ConfigInvalid => ErrorKind::InvalidInput,
        opendal::ErrorKind::NotFound => ErrorKind::NotFound,
        opendal::ErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
        opendal::ErrorKind::Unsupported => ErrorKind::Unsupported,
        _ => ErrorKind::Io,
    };
    Error::new(kind, operation, format!("Hugging Face object {path:?}")).with_source(error)
}

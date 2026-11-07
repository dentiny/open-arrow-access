//! Hugging Face repository bytes and Parquet dataset access.

mod accessor;
mod client;
mod dataset;
mod metadata;
mod options;
mod request;
mod validation;

pub use accessor::HuggingFaceAccessor;
pub use client::HuggingFaceService;
pub use options::{HuggingFaceOptions, HuggingFaceRepository};
pub use request::HuggingFaceDatasetRequest;

use http::StatusCode;
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

fn map_http_error(operation: &'static str, path: &str, status: StatusCode) -> Error {
    let kind = match status {
        StatusCode::NOT_FOUND => opendal::ErrorKind::NotFound,
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => opendal::ErrorKind::PermissionDenied,
        StatusCode::TOO_MANY_REQUESTS => opendal::ErrorKind::RateLimited,
        _ => opendal::ErrorKind::Unexpected,
    };
    map_error(
        operation,
        path,
        opendal::Error::new(kind, format!("Hub metadata HTTP {status}")),
    )
}

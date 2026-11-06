//! Hugging Face repository access with provider-owned connection settings.
//! File reads compose with shared byte and format contracts.

mod accessor;
mod client;
mod options;

pub use accessor::HuggingFaceAccessor;
pub use client::HuggingFaceService;
pub use options::{HuggingFaceOptions, HuggingFaceRepository};

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

#[cfg(test)]
mod tests;

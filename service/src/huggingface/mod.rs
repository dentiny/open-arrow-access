//! Revision-aware discovery of dataset-card file configurations.
//!
//! Services own repository/config/split selection. OpenDAL remains the internal
//! file transport; format accessors consume the resolved byte accessor unchanged.

mod accessor;
mod client;
mod options;
mod resolver;

pub use accessor::HuggingFaceAccessor;
pub use client::HuggingFaceService;
pub use options::{HuggingFaceOptions, HuggingFaceRepository};
pub use resolver::{HuggingFaceDataset, HuggingFaceSelection};

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

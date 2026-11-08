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

use open_arrow_access_core::Error;

fn map_error(operation: &'static str, path: &str, error: opendal::Error) -> Error {
    let kind = crate::opendal_error::error_kind(error.kind());
    Error::new(kind, operation, format!("Hugging Face object {path:?}")).with_source(error)
}

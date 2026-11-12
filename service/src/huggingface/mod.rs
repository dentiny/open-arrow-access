//! Hugging Face repository bytes and Parquet dataset access.

mod accessor;
mod client;
mod dataset;
mod error;
mod metadata;
mod options;
mod request;
mod validation;

pub use accessor::HuggingFaceAccessor;
pub use client::HuggingFaceService;
pub use options::{HuggingFaceOptions, HuggingFaceRepository};
pub use request::HuggingFaceDatasetRequest;

use error::map_error;

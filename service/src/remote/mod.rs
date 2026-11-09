//! Shared URL, filename and JSON field utilities for providers.
mod error;
mod validation;
pub(crate) use error::{decode, invalid};
pub use validation::{array, download_url, filename, segment, text};

mod document;
pub use document::ApiDocument;

mod connection;
mod request;
mod response;
pub use connection::Connection;

//! Provider connections, exact API response bytes, and lazy sequential downloads.
//!
//! Calls perform no background requests, automatic retries or page walks.
//! Credentials remain scoped to the configured API origin.

mod connection;
mod document;
mod download;
mod error;
mod files;
mod request;
mod response;
mod validation;

pub use crate::http_client::HttpStatusError;
pub use connection::Connection;
pub use document::ApiDocument;
pub use files::{RemoteFiles, RemoteHttpAccessor};
pub use validation::{array, filename, segment, text};

pub(crate) use error::{decode, invalid};
pub(crate) use validation::download_url;

//! Common utilities shared by provider services.
mod error;
mod json_util;
mod url_util;
mod validation;
pub(crate) use error::{decode, invalid};
pub use json_util::{array, text};
pub use url_util::{download_url, filename, segment};

mod document;
pub use document::ApiDocument;

mod connection;
mod transport;
pub use connection::Connection;

mod download;

mod files;
pub use files::{RemoteFiles, RemoteHttpAccessor};

pub use json_util::{LinkField, MetadataLink, optional_links};

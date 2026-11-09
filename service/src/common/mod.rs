//! Common utilities shared by provider services.
mod error;
mod json_util;
mod url_util;
mod validation;
pub(crate) use error::{decode, invalid};
pub use json_util::{array, text};
pub use url_util::{encode_path, filename, resolve_download_url, segment};

mod document;
pub use document::ApiDocument;

mod connection;
mod transport;
pub use connection::{Connection, RemoteFiles};

pub use json_util::{LinkField, MetadataLink, resolve_named_links, resolve_optional_links};

pub(crate) use validation::filename_component as validate_filename;

#[allow(unused_imports, reason = "used by optional providers")]
pub(crate) use validation::is_commit_sha;

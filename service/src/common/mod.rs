//! Common utilities shared by provider services.
pub(crate) mod error;
mod json_util;
mod url_util;
mod validation;
pub(crate) use error::{decode, invalid};
pub use json_util::{array, text};
pub use url_util::{encode_path, filename, resolve_download_url, segment};

mod document;
pub use document::ApiDocument;

mod connection;
pub(crate) mod transport;
pub use connection::{ApiKey, Connection, RemoteFiles};

pub use crate::discovery::{LinkField, MetadataLink, resolve_named_links, resolve_optional_links};

#[allow(
    unused_imports,
    reason = "Shared utilities are available to every provider build."
)]
pub(crate) use validation::{is_commit_sha, reject_reserved_parameters};

pub use crate::arrow::{PageSetAccessor, RecordPage, RecordPageInput};

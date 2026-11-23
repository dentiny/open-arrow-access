//! Provider metadata discovery and fixed file membership.
//! Transport performs requests; these adapters resolve metadata into lazy resources.
pub(crate) mod file_manifest;
mod links;
mod metadata;
mod remote_files;
mod resolved_files;

pub use links::{LinkField, MetadataLink, resolve_named_links, resolve_optional_links};
pub use resolved_files::ResolvedFileSet;

pub use file_manifest::ResolvedFile;
pub use metadata::{HttpFileRoot, ResolvedMetadata};

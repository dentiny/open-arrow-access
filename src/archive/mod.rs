//! Provider-independent expansion of source packages into binary member rows.
//!
//! This integrated adapter composes an existing byte accessor, optional outer
//! decompression, sequential TAR traversal, header filtering and Arrow batching.
//! It is the package-reading counterpart of `CrawlAccessor`: storage and business
//! policy remain outside the adapter. It never interprets article IDs, selects
//! TeX automatically, validates text, extracts filesystem paths, or recurses into
//! nested archives. Member names and payload bytes remain opaque.

mod accessor;
mod error;
mod options;
mod reader;
mod validation;

pub use accessor::ArchiveAccessor;
pub use options::{ArchiveFormat, ArchiveOptions};
pub use reader::archive_schema;

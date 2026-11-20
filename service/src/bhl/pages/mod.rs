//! Joined, selected AWS OCR page records. See `service/tools/bhl_metadata.py`
//! for the disk-backed whole-corpus preparation and per-shard sidecar contract.
mod accessor;
mod identity;
mod metadata;
mod schema;

pub use accessor::BhlPageAccessor;
pub use identity::BhlPageIdentity;
pub use metadata::{BhlJoinReport, BhlMetadata};
pub use schema::page_schema;

use super::error::invalid_pages as invalid;

#[cfg(test)]
mod tests;

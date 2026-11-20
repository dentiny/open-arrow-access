//! AWS OCR page identity and records with caller-supplied structured metadata.
mod accessor;
pub(super) mod identity;

use super::error::invalid_pages as invalid;
pub(super) use super::page_schema;
pub use accessor::BhlPageAccessor;
pub use identity::BhlPageIdentity;

#[cfg(test)]
mod tests;

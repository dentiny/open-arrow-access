//! AWS OCR page identity and output schema; metadata is declared but not populated.
mod accessor;
pub(super) mod identity;

use super::error::invalid_pages as invalid;
pub(super) use super::page_schema;
pub use accessor::BhlPageAccessor;
pub use identity::BhlPageIdentity;

#[cfg(test)]
mod tests;

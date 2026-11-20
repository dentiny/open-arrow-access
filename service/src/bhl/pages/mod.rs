//! AWS OCR page identity and output schema; metadata is declared but not populated.
mod accessor;
mod identity;
mod schema;

use super::error::invalid_pages as invalid;
pub use accessor::BhlPageAccessor;
pub use identity::BhlPageIdentity;
pub use schema::page_schema;

#[cfg(test)]
mod tests;

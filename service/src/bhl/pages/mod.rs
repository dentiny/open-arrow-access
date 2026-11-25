//! AWS OCR page identity and records with caller-supplied structured metadata.
mod accessor;
pub(super) mod identity;
pub(in crate::bhl) mod reconciliation;

use super::error::invalid_pages as invalid;
pub(super) use super::page_schema;
pub use accessor::BhlPageAccessor;
pub use identity::BhlPageIdentity;
pub use reconciliation::BhlPageReconciliation;

#[cfg(test)]
mod projection_tests;
#[cfg(test)]
mod reconciliation_tests;
#[cfg(test)]
mod tests;

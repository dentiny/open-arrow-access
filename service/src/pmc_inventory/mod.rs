//! Read one PMC inventory snapshot without fetching articles.
mod accessor;
mod error;
mod manifest;
mod paths;
mod reader;
mod validation;

pub use accessor::PmcInventoryAccessor;
pub(crate) use validation::validate_shard_selection;

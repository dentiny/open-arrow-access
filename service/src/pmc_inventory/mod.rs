//! Read one PMC inventory snapshot without fetching articles.
mod accessor;
mod error;
mod manifest;
mod reader;
mod validation;

pub use accessor::PmcInventoryAccessor;

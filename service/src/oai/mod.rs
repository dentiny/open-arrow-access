//! Shared ListRecords protocol preserving exact XML and deleted records.
mod error;
mod page;
mod protocol;
mod validation;
pub use page::OaiPage;
pub use protocol::OaiService;

mod provider;
pub use provider::{OaiProvider, OaiProviderService};

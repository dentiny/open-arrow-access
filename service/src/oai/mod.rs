//! Shared ListRecords protocol preserving exact XML and deleted records.
//!
//! Bind an already fetched page with `page.records(format)` to obtain an Arrow
//! reader and scan plan with an explicit XML record path/schema. arXiv metadata,
//! DOAB and OAPEN reuse this path. Configured providers expose `record_format()`
//! for stable standard header columns, including empty pages. Resumption-token
//! acquisition stays sequential and is not treated as independently fetchable
//! parallel work.
mod error;
mod page;
mod parser;
mod protocol;
mod record_format;
mod validation;
pub use page::OaiPage;
pub use protocol::OaiService;

mod provider;
pub use provider::{OaiProvider, OaiProviderService};

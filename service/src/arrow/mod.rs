//! Arrow adapters over already resolved pages and files.
//! Provider discovery and pagination do not select a decoder or Arrow schema.
mod files;
mod json;
#[cfg(feature = "oai")]
mod oai;
mod page_set;
mod record_page;

pub use page_set::PageSetAccessor;
pub use record_page::{RecordPage, RecordPageInput};

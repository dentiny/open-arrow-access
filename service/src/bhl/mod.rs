//! BHL AWS Open Data objects and page records.
mod error;
#[cfg(feature = "bhl-metadata")]
pub mod metadata;
mod open_data;
mod page;
mod pages;
pub use page::{BhlPageRecord, page_schema};
pub use pages::{BhlPageAccessor, BhlPageIdentity};
mod validation;
pub use open_data::{BhlOpenData, OPEN_DATA_BUCKET_URL, open_data_bucket_operator};

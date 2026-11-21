//! BHL AWS Open Data objects and page records.
mod error;
mod inventory;
pub use inventory::{BhlInventory, BhlOcrScope, BhlPageJob};
pub mod metadata;
mod open_data;
mod page;
mod pages;
pub use page::{BhlPageRecord, page_schema};
pub use pages::{BhlPageAccessor, BhlPageIdentity};
mod validation;
pub use open_data::{BhlOpenData, OPEN_DATA_BUCKET_URL, open_data_bucket_operator};

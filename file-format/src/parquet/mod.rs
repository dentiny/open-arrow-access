//! Single-file Parquet access over the shared byte-reader contract.

mod accessor;
mod error;
mod format;
mod input;
mod planning;
mod validation;

pub use accessor::ParquetAccessor;
pub use format::ParquetFormat;

use error::decode_error;

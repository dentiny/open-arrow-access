//! Single-file Parquet access over the shared byte-reader contract.

mod accessor;
mod format;
mod input;
mod planning;

pub use accessor::ParquetAccessor;
pub use format::ParquetFormat;

use open_arrow_access_core::{Error, ErrorKind};

fn decode_error<E>(operation: &'static str, error: E) -> Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    crate::error::source_error(operation, ErrorKind::Decode, error)
}

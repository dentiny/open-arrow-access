//! Single-file Parquet access over the shared byte-reader contract.

mod accessor;
mod input;
mod reader;

pub use accessor::ParquetAccessor;

use open_arrow_access_core::{Error, ErrorKind};

fn decode_error<E>(operation: &'static str, error: E) -> Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    Error::new(ErrorKind::Decode, operation, error.to_string()).with_source(error)
}

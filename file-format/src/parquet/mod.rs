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
    let mut source: &(dyn std::error::Error + 'static) = &error;
    let kind = loop {
        if let Some(input_error) = source.downcast_ref::<Error>() {
            break input_error.kind();
        }
        match source.source() {
            Some(inner) => source = inner,
            None => break ErrorKind::Decode,
        }
    };
    Error::new(kind, operation, error.to_string()).with_source(error)
}

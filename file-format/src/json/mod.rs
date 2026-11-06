//! JSON object streams, including JSONL, decoded with an explicit Arrow schema.

mod accessor;
mod reader;

pub use accessor::JsonAccessor;

use arrow_schema::ArrowError;
use open_arrow_access_core::{Error, ErrorKind};

fn decode_error(error: ArrowError) -> Error {
    Error::new(ErrorKind::Decode, "decode_json", error.to_string()).with_source(error)
}

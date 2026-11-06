mod accessor;
mod reader;

pub use accessor::CsvAccessor;
pub use arrow_csv::reader::Format as CsvFormat;

use open_arrow_access_core::{Error, ErrorKind};

fn decode_error(error: arrow_schema::ArrowError) -> Error {
    Error::new(ErrorKind::Decode, "decode_csv", error.to_string()).with_source(error)
}

#[cfg(test)]
mod tests;

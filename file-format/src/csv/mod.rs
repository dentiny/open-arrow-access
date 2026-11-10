mod accessor;
mod dialect;
mod planning;
pub use dialect::CsvDialect;
mod format;
mod reader;

pub use accessor::CsvAccessor;
pub use arrow_csv::reader::Format as CsvFormat;
pub use format::CsvFileFormat;

use open_arrow_access_core::{Error, ErrorKind};

fn decode_error(error: arrow_schema::ArrowError) -> Error {
    Error::new(ErrorKind::Decode, "decode_csv", error.to_string()).with_source(error)
}

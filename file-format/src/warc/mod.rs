mod accessor;
mod arrow;
mod format;
pub use arrow::{WarcArrowAccessor, WarcArrowFormat, arrow_schema};

pub use accessor::WarcAccessor;
pub use format::WarcFormat;
pub use warc::WarcHeader;

/// An owned record from the upstream parser, with a buffered, unmodified body.
pub type WarcRecord = warc::Record<warc::BufferedBody>;

use std::error::Error as StdError;

use open_arrow_access_core::{Error, ErrorKind};

fn decode_error(error: impl StdError + Send + Sync + 'static) -> Error {
    crate::error::source_error("decode_warc", ErrorKind::Decode, error)
}

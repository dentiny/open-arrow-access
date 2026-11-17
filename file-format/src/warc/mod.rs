mod accessor;
mod arrow;
mod error;
mod format;
mod planning;
pub use arrow::{WarcArrowAccessor, WarcArrowFormat, arrow_schema};

pub use accessor::WarcAccessor;
pub use format::WarcFormat;
pub use warc::WarcHeader;

/// An owned record from the upstream parser, with a buffered, unmodified body.
pub type WarcRecord = warc::Record<warc::BufferedBody>;

use error::decode_error;

mod accessor;
mod batch;
mod reader;
mod schema;

pub use accessor::WarcAccessor;
pub use schema::warc_schema;

use std::error::Error as StdError;

use open_arrow_access_core::{Error, ErrorKind};

fn decode_error(error: impl StdError + Send + Sync + 'static) -> Error {
    Error::new(
        ErrorKind::from_source(&error).unwrap_or(ErrorKind::Decode),
        "decode_warc",
        error.to_string(),
    )
    .with_source(error)
}

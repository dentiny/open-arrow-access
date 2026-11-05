mod accessor;
mod batch;
mod reader;
mod schema;

pub use accessor::WarcAccessor;
pub use schema::warc_schema;

use std::{error::Error as StdError, io};

use open_arrow_access_core::{Error, ErrorKind};

fn decode_error(error: impl StdError + Send + Sync + 'static) -> Error {
    Error::new(
        find_source_kind(&error).unwrap_or(ErrorKind::Decode),
        "decode_warc",
        error.to_string(),
    )
    .with_source(error)
}

fn find_source_kind(error: &(dyn StdError + 'static)) -> Option<ErrorKind> {
    if let Some(error) = error.downcast_ref::<Error>() {
        return Some(error.kind());
    }
    if let Some(inner) = error
        .downcast_ref::<io::Error>()
        .and_then(io::Error::get_ref)
        && let Some(kind) = find_source_kind(inner)
    {
        return Some(kind);
    }
    error.source().and_then(find_source_kind)
}

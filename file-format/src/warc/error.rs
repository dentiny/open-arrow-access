use open_arrow_access_core::{Error, ErrorKind};
use std::error::Error as StdError;

pub(super) fn decode_error(error: impl StdError + Send + Sync + 'static) -> Error {
    crate::error::source_error("decode_warc", ErrorKind::Decode, error)
}

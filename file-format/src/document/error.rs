use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn row_count() -> Error {
    Error::new(
        ErrorKind::Decode,
        "read_format_document",
        "expected exactly one document row",
    )
}

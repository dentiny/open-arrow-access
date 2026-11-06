use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn schema_mismatch() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "read_file_set",
        "file or batch schema differs from the fixed dataset schema",
    )
}

use arrow_schema::ArrowError;
use open_arrow_access_core::{Error, ErrorKind};

pub(crate) fn column_error(
    kind: ErrorKind,
    operation: &'static str,
    column: &str,
    error: ArrowError,
) -> Error {
    Error::new(kind, operation, format!("column {column:?}: {error}")).with_source(error)
}

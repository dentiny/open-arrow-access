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

pub(crate) fn schema_error() -> Error {
    Error::new(
        ErrorKind::Decode,
        "parallel_read",
        "partition schema differs from scan schema",
    )
}

pub(crate) fn partition_task_error(error: tokio::task::JoinError) -> Error {
    Error::new(
        ErrorKind::Decode,
        "parallel_read",
        format!("partition task failed: {error}"),
    )
}

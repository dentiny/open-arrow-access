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

pub(crate) fn invalid_generated_column(column: &str, message: impl std::fmt::Display) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "plan_generated_column",
        format!("column {column:?}: {message}"),
    )
}

pub(crate) fn invalid_generated_expression(message: &str) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Plan(message.into())
}

pub(crate) fn generated_plan_error(
    column: &str,
    error: datafusion::error::DataFusionError,
) -> Error {
    invalid_generated_column(column, &error).with_source(error)
}

pub(crate) fn generated_column_error(
    column: &str,
    error: datafusion::error::DataFusionError,
) -> Error {
    Error::new(
        ErrorKind::Decode,
        "generate_column",
        format!("column {column:?}: {error}"),
    )
    .with_source(error)
}

pub(crate) fn generated_batch_error(error: ArrowError) -> Error {
    Error::new(ErrorKind::Decode, "generate_column", error.to_string()).with_source(error)
}

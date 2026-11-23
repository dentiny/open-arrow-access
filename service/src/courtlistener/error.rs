use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn invalid() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "courtlistener_snapshot",
        "expected a lowercase table name and a valid YYYY-MM-DD date (bulk-data/<table>-<date>.csv.bz2)",
    )
}

pub(super) fn records_error(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Decode, "courtlistener_records", message)
}
pub(super) fn invalid_value(context: &str, field: &arrow_schema::Field) -> Error {
    records_error(format!(
        "{context} field {}: invalid {}",
        field.name(),
        field.data_type()
    ))
}
pub(super) fn schema_error(message: &str) -> Error {
    Error::new(ErrorKind::InvalidInput, "courtlistener_schema", message)
}
pub(super) fn invalid_dialect() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "courtlistener_copy",
        "invalid COPY quote/escape character",
    )
}
pub(super) fn copy_error(object: &str, ordinal: u64, message: impl std::fmt::Display) -> Error {
    Error::new(
        ErrorKind::Decode,
        "courtlistener_copy",
        format!("{object} record {ordinal}: {message}"),
    )
}
pub(super) fn arrow_error(e: arrow_schema::ArrowError) -> Error {
    records_error(e.to_string()).with_source(e)
}

pub(super) use crate::duckdb::error::database as db_error;

pub(super) fn task_error(context: &'static str, e: tokio::task::JoinError) -> Error {
    Error::new(ErrorKind::Internal, context, e.to_string()).with_source(e)
}

use open_arrow_access_core::{Error, ErrorKind};

/// Preserve an upstream shared category, falling back to the format's classification.
#[cfg(any(feature = "parquet", feature = "warc", feature = "xml"))]
pub(crate) fn source_error(
    operation: &'static str,
    fallback: ErrorKind,
    source: impl std::error::Error + Send + Sync + 'static,
) -> Error {
    let kind = ErrorKind::from_source(&source).unwrap_or(fallback);
    Error::new(kind, operation, source.to_string()).with_source(source)
}

#[cfg(any(feature = "xml", feature = "json", feature = "csv"))]
pub(crate) fn document_row_count() -> Error {
    Error::new(
        ErrorKind::Decode,
        "read_format_document",
        "expected exactly one document row",
    )
}

fn invalid_source_columns(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidInput, "source_columns", message)
}
pub(crate) fn source_object_collision() -> Error {
    invalid_source_columns("source object column conflicts with data field")
}
pub(crate) fn missing_source_field() -> Error {
    invalid_source_columns("missing source field")
}
pub(crate) fn source_column_batch_error(error: arrow_schema::ArrowError) -> Error {
    invalid_source_columns(error.to_string())
}
#[cfg(feature = "warc")]
pub(crate) fn source_column_conflict() -> Error {
    invalid_source_columns("source column conflicts with data field")
}
#[cfg(feature = "warc")]
pub(crate) fn source_position_unavailable(
    object: Option<&open_arrow_access_core::ObjectRef>,
) -> Error {
    Error::new(
        ErrorKind::Unsupported,
        "read_warc",
        "byte input does not provide source positions",
    )
    .with_object(object)
}
#[cfg(feature = "warc")]
pub(crate) fn source_position_lost() -> Error {
    Error::new(
        ErrorKind::Unsupported,
        "read_warc",
        "byte input lost source positions",
    )
}

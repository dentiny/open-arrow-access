use open_arrow_access_core::{Error, ErrorKind};

/// Preserve an upstream shared category, falling back to the format's classification.
pub(crate) fn source_error(
    operation: &'static str,
    fallback: ErrorKind,
    source: impl std::error::Error + Send + Sync + 'static,
) -> Error {
    let kind = ErrorKind::from_source(&source).unwrap_or(fallback);
    Error::new(kind, operation, source.to_string()).with_source(source)
}

#[cfg(feature = "parquet")]
pub(crate) fn parquet_partition_input_error() -> Error {
    Error::new(
        ErrorKind::Unsupported,
        "plan_parquet",
        "Parquet partitions require range reads and a known byte length",
    )
}

#[cfg(feature = "parquet")]
pub(crate) fn parquet_partition_changed() -> Error {
    Error::new(
        ErrorKind::Conflict,
        "open_parquet_partition",
        "input length or version changed after row-group planning",
    )
}

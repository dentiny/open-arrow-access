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

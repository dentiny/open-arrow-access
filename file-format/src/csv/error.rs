use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn decode_error(error: arrow_schema::ArrowError) -> Error {
    Error::new(ErrorKind::Decode, "decode_csv", error.to_string()).with_source(error)
}

pub(super) fn input_error(source: std::io::Error) -> Error {
    let kind = ErrorKind::from_source(&source).unwrap_or(ErrorKind::Io);
    Error::new(kind, "frame_csv", source.to_string()).with_source(source)
}

pub(super) fn record_error(source: csv_async::Error) -> Error {
    let kind = match source.kind() {
        csv_async::ErrorKind::Io(error) => ErrorKind::from_source(error).unwrap_or(ErrorKind::Io),
        _ => ErrorKind::Decode,
    };
    Error::new(kind, "frame_csv", source.to_string()).with_source(source)
}

pub(super) fn write_error(source: csv::Error) -> Error {
    Error::new(ErrorKind::Decode, "frame_csv", source.to_string()).with_source(source)
}

pub(super) fn inference_error(source: arrow_schema::ArrowError) -> Error {
    Error::new(ErrorKind::Decode, "infer_csv", source.to_string()).with_source(source)
}

pub(super) fn invalid_inference_limits() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "infer_csv",
        "inference record and byte limits must be positive",
    )
}

pub(super) fn empty_inference_sample() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "infer_csv",
        "cannot infer a schema from empty CSV input; supply an explicit schema",
    )
}

pub(super) fn invalid_inferred_headers() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "infer_csv",
        "CSV headers must be nonempty and distinct",
    )
}

pub(super) fn null_inferred_column() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "infer_csv",
        "cannot infer types for header-only or all-null CSV columns; supply an explicit schema",
    )
}

pub(super) fn uninspectable_dialect() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "infer_csv",
        "schema inference and automatic header detection require an inspectable CsvDialect; use with_dialect instead of with_format",
    )
}

pub(super) fn empty_input_schema() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "open_csv",
        "CSV requires an input schema with at least one field",
    )
}

pub(super) fn unsupported_field(field: &arrow_schema::Field) -> Error {
    Error::new(
        ErrorKind::Unsupported,
        "open_csv",
        format!(
            "CSV does not support field {:?} of type {:?}",
            field.name(),
            field.data_type()
        ),
    )
}

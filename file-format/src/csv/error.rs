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

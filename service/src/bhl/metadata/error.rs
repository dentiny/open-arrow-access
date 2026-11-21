use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidInput, "bhl_metadata", message)
}

pub(super) fn json(source: serde_json::Error) -> Error {
    Error::new(ErrorKind::Decode, "bhl_metadata", source.to_string()).with_source(source)
}

pub(super) fn duckdb(source: duckdb::Error) -> Error {
    Error::new(ErrorKind::Io, "bhl_metadata_duckdb", source.to_string()).with_source(source)
}
pub(super) fn arrow(source: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::new(ErrorKind::Decode, "bhl_metadata_arrow", source.to_string()).with_source(source)
}
pub(super) fn task(source: tokio::task::JoinError) -> Error {
    Error::new(ErrorKind::Internal, "bhl_metadata", source.to_string()).with_source(source)
}

pub(super) fn snapshot_io(source: std::io::Error) -> Error {
    Error::new(
        ErrorKind::Io,
        "bhl_metadata_snapshot",
        "metadata snapshot I/O failed",
    )
    .with_source(source)
}

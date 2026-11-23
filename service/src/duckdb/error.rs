use open_arrow_access_core::{Error, ErrorKind};

pub(crate) fn database(source: ::duckdb::Error) -> Error {
    Error::new(ErrorKind::Io, "duckdb", source.to_string()).with_source(source)
}

pub(super) fn scratch(source: std::io::Error) -> Error {
    Error::new(ErrorKind::Io, "duckdb_scratch", source.to_string()).with_source(source)
}

#[cfg(feature = "duckdb")]
pub(super) fn length_mismatch() -> Error {
    Error::new(
        ErrorKind::Io,
        "duckdb_download",
        "database file length differs from expected length",
    )
}

#[cfg(feature = "duckdb")]
pub(super) fn task(source: tokio::task::JoinError) -> Error {
    Error::new(ErrorKind::Internal, "duckdb_open", source.to_string()).with_source(source)
}

#[cfg(feature = "duckdb")]
pub(super) fn secret_setup() -> Error {
    Error::new(
        ErrorKind::Io,
        "duckdb_secret_setup",
        "could not read or execute secret SQL file",
    )
}

#[cfg(feature = "duckdb")]
pub(super) fn attach() -> Error {
    Error::new(
        ErrorKind::Io,
        "duckdb_attach",
        "could not attach database read-only",
    )
}

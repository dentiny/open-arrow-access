use open_arrow_access_core::{Error, ErrorKind};

pub(crate) fn database(source: ::duckdb::Error) -> Error {
    Error::new(ErrorKind::Io, "duckdb", source.to_string()).with_source(source)
}

pub(super) fn scratch(source: std::io::Error) -> Error {
    Error::new(ErrorKind::Io, "duckdb_scratch", source.to_string()).with_source(source)
}

pub(super) fn cache_setup(source: ::duckdb::Error) -> Error {
    Error::new(
        ErrorKind::Io,
        "duckdb_cache",
        "could not install/load cache_httpfs or configure its on-disk cache",
    )
    .with_source(source)
}

pub(super) fn init_setup() -> Error {
    Error::new(
        ErrorKind::Io,
        "duckdb_init_setup",
        "could not execute initialization SQL",
    )
}

pub(super) fn attach() -> Error {
    Error::new(
        ErrorKind::Io,
        "duckdb_attach",
        "could not attach database read-only",
    )
}

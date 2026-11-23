use open_arrow_access_core::{Error, ErrorKind};

pub(crate) fn database(source: ::duckdb::Error) -> Error {
    Error::new(ErrorKind::Io, "duckdb", source.to_string()).with_source(source)
}

pub(super) fn scratch(source: std::io::Error) -> Error {
    Error::new(ErrorKind::Io, "duckdb_scratch", source.to_string()).with_source(source)
}

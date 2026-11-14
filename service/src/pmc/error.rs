use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn decode(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Decode, "read_pmc", message)
}

pub(super) fn record_batch(source: arrow_schema::ArrowError) -> Error {
    decode("cannot assemble PMC metadata row").with_source(source)
}

use open_arrow_access_core::{Error, ErrorKind, ObjectRef};

pub(super) fn decode_error(
    object: Option<&ObjectRef>,
    record_index: u64,
    source: impl std::error::Error + Send + Sync + 'static,
) -> Error {
    let locator = object.map_or("<input>", ObjectRef::locator);
    Error::new(
        ErrorKind::Decode,
        "read_atif",
        format!("{locator}: record {record_index}: {source}"),
    )
    .with_source(source)
}

pub(super) fn empty_chunk_error() -> Error {
    Error::new(
        ErrorKind::Io,
        "read_atif",
        "byte reader returned an empty chunk",
    )
}

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

pub(super) fn expected_object(object: Option<&ObjectRef>, record_index: u64) -> Error {
    decode_error(
        object,
        record_index,
        std::io::Error::new(std::io::ErrorKind::InvalidData, "expected a JSON object"),
    )
}

pub(super) fn empty_chunk() -> Error {
    Error::new(
        ErrorKind::Io,
        "read_atif",
        "byte reader emitted an empty chunk",
    )
}

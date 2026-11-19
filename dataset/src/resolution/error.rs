use open_arrow_access_core::{Error, ErrorKind, ObjectRef};
use open_arrow_access_file_format::DetectedFormat;

pub(super) fn mixed(
    first: &ObjectRef,
    format: DetectedFormat,
    other: &ObjectRef,
    other_format: DetectedFormat,
) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "resolve_files",
        format!(
            "selection mixes {format:?} file {:?} and {other_format:?} file {:?}",
            first.locator(),
            other.locator()
        ),
    )
}

pub(super) fn atif_preference() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "resolve_files",
        "ATIF requires content detection and cannot be a filename preference",
    )
}

pub(super) fn empty_selection() -> Error {
    Error::new(
        ErrorKind::NotFound,
        "resolve_files",
        "selection contains no recognized files",
    )
}

pub(super) fn empty_chunk(object: &ObjectRef) -> Error {
    Error::new(
        ErrorKind::Decode,
        "probe_json_format",
        "byte reader returned an empty chunk",
    )
    .with_object(Some(object))
}

pub(super) fn probe_limit(object: &ObjectRef) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "probe_json_format",
        "no complete JSON record within the probe byte limit",
    )
    .with_object(Some(object))
}

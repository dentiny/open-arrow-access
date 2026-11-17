use super::JsonKind;
use open_arrow_access_core::{Error, ErrorKind, ObjectRef};

pub(super) fn resolve(
    kind: ErrorKind,
    message: impl Into<String>,
    object: Option<&ObjectRef>,
) -> Error {
    Error::new(kind, "resolve_json_format", message).with_object(object)
}

pub(super) fn empty_selection(object: Option<&ObjectRef>) -> Error {
    resolve(
        ErrorKind::NotFound,
        "JSON selection contains only empty files",
        object,
    )
}

pub(super) fn mixed_kinds(
    first: JsonKind,
    example: &ObjectRef,
    other: JsonKind,
    object: &ObjectRef,
) -> Error {
    resolve(
        ErrorKind::InvalidInput,
        format!(
            "mixed JSON kinds {first:?} ({:?}) and {other:?} ({:?})",
            example.locator(),
            object.locator(),
        ),
        Some(object),
    )
}

pub(super) fn unsupported_schema(object: Option<&ObjectRef>) -> Error {
    resolve(
        ErrorKind::InvalidInput,
        "explicit schema is not supported for ATIF's fixed schema",
        object,
    )
}

pub(super) fn disabled_reader(kind: JsonKind, object: Option<&ObjectRef>) -> Error {
    resolve(
        ErrorKind::Unsupported,
        format!("reader feature is disabled for {kind:?}"),
        object,
    )
}

pub(super) fn invalid_json(source: serde_json::Error, object: &ObjectRef) -> Error {
    resolve(ErrorKind::Decode, source.to_string(), Some(object)).with_source(source)
}

pub(super) fn incomplete_json(object: &ObjectRef) -> Error {
    resolve(
        ErrorKind::Decode,
        "incomplete JSON value while resolving content type",
        Some(object),
    )
}

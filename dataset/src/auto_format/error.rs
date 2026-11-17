use open_arrow_access_core::{Error, ErrorKind, ObjectRef};

use super::{DetectedFormat, ResolvedKind};

pub(super) fn no_dataset_files() -> Error {
    Error::new(
        ErrorKind::NotFound,
        "resolve_auto_format",
        "no dataset files",
    )
}

pub(super) fn unsupported_schema(object: Option<&ObjectRef>) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "resolve_auto_format",
        "explicit schema is supported only for CSV and ordinary JSON",
    )
    .with_object(object)
}

pub(super) fn disabled_reader(kind: ResolvedKind, object: Option<&ObjectRef>) -> Error {
    Error::new(
        ErrorKind::Unsupported,
        "resolve_auto_format",
        format!("reader feature is disabled for {kind:?}"),
    )
    .with_object(object)
}

pub(super) fn no_recognized_files(object: Option<&ObjectRef>) -> Error {
    Error::new(
        ErrorKind::NotFound,
        "select_format",
        "no recognized dataset files",
    )
    .with_object(object)
}

pub(super) fn mixed_formats(
    first: DetectedFormat,
    example: &ObjectRef,
    other: DetectedFormat,
    object: &ObjectRef,
) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "select_format",
        format!(
            "mixed formats {first:?} ({:?}) and {other:?} ({:?})",
            example.locator(),
            object.locator()
        ),
    )
    .with_object(Some(object))
}

pub(super) fn resolve(
    kind: ErrorKind,
    message: impl Into<String>,
    object: Option<&ObjectRef>,
) -> Error {
    Error::new(kind, "resolve_auto_format", message).with_object(object)
}

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

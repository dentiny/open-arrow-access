use async_zip::error::ZipError;
use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn zip_error(error: ZipError) -> Error {
    let kind = ErrorKind::from_source(&error).unwrap_or(match error {
        ZipError::FeatureNotSupported(_)
        | ZipError::CompressionNotSupported(_)
        | ZipError::TargetZip64NotSupported => ErrorKind::Unsupported,
        _ => ErrorKind::Decode,
    });
    Error::new(kind, "read_zip", "ZIP archive operation failed").with_source(error)
}

pub(super) fn entry_not_found() -> Error {
    Error::new(
        ErrorKind::NotFound,
        "open_zip_entry",
        "ZIP entry index is out of bounds",
    )
}

pub(super) fn invalid_header_offset() -> Error {
    Error::new(
        ErrorKind::Decode,
        "open_zip_entry",
        "invalid local header offset",
    )
}

pub(super) fn encrypted_entry() -> Error {
    Error::new(
        ErrorKind::Unsupported,
        "open_zip_entry",
        "encrypted ZIP entries are unsupported",
    )
}

pub(super) fn missing_zip64_offset() -> Error {
    Error::new(
        ErrorKind::Decode,
        "read_zip",
        "ZIP64 local header offset is missing",
    )
}

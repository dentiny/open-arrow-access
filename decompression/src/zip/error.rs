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

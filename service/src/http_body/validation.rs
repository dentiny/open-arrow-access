use super::error::body_error;
use open_arrow_access_core::Result;

pub(super) fn validate_length(
    read: u64,
    expected: Option<u64>,
    operation: &'static str,
) -> Result<()> {
    if expected.is_some_and(|length| read > length) {
        return Err(body_error(operation, "HTTP body exceeds Content-Length"));
    }
    Ok(())
}

pub(super) fn validate_complete(
    read: u64,
    expected: Option<u64>,
    operation: &'static str,
) -> Result<()> {
    if expected.is_some_and(|length| read != length) {
        return Err(body_error(operation, "truncated HTTP body"));
    }
    Ok(())
}

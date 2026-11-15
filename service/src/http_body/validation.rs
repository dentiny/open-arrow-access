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

pub(super) fn validate_collected_bytes(
    current: usize,
    additional: usize,
    limit: Option<usize>,
    operation: &'static str,
) -> Result<()> {
    if current
        .checked_add(additional)
        .is_none_or(|total| limit.is_some_and(|limit| total > limit))
    {
        return Err(super::error::body_limit_exceeded(operation, limit));
    }
    Ok(())
}

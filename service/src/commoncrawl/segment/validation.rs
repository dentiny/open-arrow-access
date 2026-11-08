use open_arrow_access_core::Result;

use super::error::invalid_segment;

pub(super) fn validate_segment(segment: &str) -> Result<()> {
    let message = "expected a numeric timestamp.part segment identifier";
    let Some((timestamp, part)) = segment.split_once('.') else {
        return Err(invalid_segment(message));
    };
    if timestamp.is_empty() || !timestamp.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid_segment(message));
    }
    if part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid_segment(message));
    }
    Ok(())
}

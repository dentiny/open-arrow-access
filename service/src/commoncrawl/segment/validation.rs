use open_arrow_access_core::Result;

use super::error::invalid_segment;

pub(super) fn validate_segment(segment: &str) -> Result<()> {
    if !matches!(segment.split_once('.'), Some((timestamp, part)) if !timestamp.is_empty() && !part.is_empty() && timestamp.bytes().all(|byte| byte.is_ascii_digit()) && part.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return Err(invalid_segment(
            "expected a numeric timestamp.part segment identifier",
        ));
    }
    Ok(())
}

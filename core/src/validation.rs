use std::ops::Range;

use crate::{Error, ErrorKind, Result};

/// Validate a half-open byte range against the current representation's length.
/// Empty ranges at or before EOF are valid. This performs no I/O and leaves
/// short-read detection to the caller; the supplied operation is kept in errors.
pub fn validate_byte_range(range: &Range<u64>, len: u64, operation: &'static str) -> Result<()> {
    if range.start > range.end || range.end > len {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            operation,
            format!(
                "range {}..{} is outside object length {}",
                range.start, range.end, len,
            ),
        ));
    }
    Ok(())
}

/// Check slash-separated relative path components without normalizing them.
/// Empty, dot and parent components (including leading/trailing slashes) fail.
/// Whitespace, backslashes, control characters and URL syntax are left to callers.
pub fn is_relative_path(path: &str) -> bool {
    path.split('/').all(|part| !matches!(part, "" | "." | ".."))
}

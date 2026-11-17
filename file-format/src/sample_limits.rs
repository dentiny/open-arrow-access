use open_arrow_access_core::{Error, ErrorKind, Result};

#[cfg(any(feature = "csv", feature = "json"))]
pub(crate) const DEFAULT_MAX_RECORDS: usize = 1000;
#[cfg(any(feature = "csv", feature = "json"))]
pub(crate) const DEFAULT_MAX_BYTES: usize = 64 * 1024 * 1024;

// Formats retain their own sampling units and record-boundary behavior.
pub(crate) fn validate_sample_limits(
    max_records: usize,
    max_bytes: usize,
    operation: &'static str,
) -> Result<()> {
    if max_records == 0 || max_bytes == 0 {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            operation,
            "inference record and byte limits must be positive",
        ));
    }
    Ok(())
}

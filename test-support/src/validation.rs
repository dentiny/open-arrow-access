use open_arrow_access_core::Result;
use std::ops::Range;
pub(crate) fn validate_listing(enabled: bool) -> Result<()> {
    if !enabled {
        return Err(crate::error::listing_disabled());
    }
    Ok(())
}

pub(crate) fn validate_range(enabled: bool, range: &Range<u64>, length: u64) -> Result<()> {
    if !enabled {
        return Err(crate::error::sequential_only());
    }
    open_arrow_access_core::validate_byte_range(range, length, "read_range")
        .map_err(|_| crate::error::invalid_range())
}

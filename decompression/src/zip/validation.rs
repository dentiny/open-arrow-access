use async_zip::spec::{
    constructs::CDR,
    extra::{EFD, EFHID},
};
use open_arrow_access_core::{Error, ErrorKind, Result};

/// async_zip 0.0.19 unwraps this optional ZIP64 field in lfh_offset().
/// Validate it before any entry can reach that upstream accessor.
pub(super) fn validate_offset(entry: &CDR) -> Result<()> {
    if entry.cdrh.lfh_offset == u32::MAX
        && entry
            .find_ef(EFHID::EI64)
            .and_then(|field| match &field.efd {
                EFD::EI64(data) => data.relative_offset,
                _ => None,
            })
            .is_none()
    {
        return Err(Error::new(
            ErrorKind::Decode,
            "read_zip",
            "ZIP64 local header offset is missing",
        ));
    }
    Ok(())
}

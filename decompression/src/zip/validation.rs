use super::error::zip_error;
use async_zip::spec::{
    constructs::CDR,
    extra::{EFD, EFHID},
};
use open_arrow_access_core::Result;

/// Every central-directory entry needs at least a 46-byte fixed header. Bound
/// declarations before async_zip preallocates both its CDR and offset vectors;
/// each vector's byte capacity must also fit Rust's isize allocation limit.
pub(super) fn directory_entry_limit(archive_len: u64) -> u64 {
    let entry_size = std::mem::size_of::<CDR>().max(std::mem::size_of::<u64>());
    let addressable_entries = (isize::MAX as usize / entry_size) as u64;
    (archive_len / 46).min(addressable_entries)
}

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
        return Err(super::error::missing_zip64_offset());
    }
    Ok(())
}

pub(super) fn header_flags_offset(entry: &CDR, length: u64) -> Result<u64> {
    let flags_offset = entry
        .lfh_offset()
        .map_err(zip_error)?
        .checked_add(6)
        .filter(|offset| offset.checked_add(2).is_some_and(|end| end <= length))
        .ok_or_else(super::error::invalid_header_offset)?;

    Ok(flags_offset)
}

pub(super) fn validate_encryption(flags: u8) -> Result<()> {
    if flags & 0x41 != 0 {
        return Err(super::error::encrypted_entry());
    }
    Ok(())
}

use open_arrow_access_core::Result;

use super::{
    ArxivArchive,
    error::{decode, invalid},
};

pub(super) fn validate_month(month: u16) -> Result<()> {
    if month > 9999 {
        return Err(invalid("arXiv month must be encoded as YYMM"));
    }
    if !(1..=12).contains(&(month % 100)) {
        return Err(invalid("arXiv month must have MM between 01 and 12"));
    }
    Ok(())
}

pub(super) fn validate_archive(archive: &ArxivArchive) -> Result<()> {
    validate_month(archive.month).map_err(|_| decode("invalid archive month"))?;
    let prefix = archive
        .kind
        .prefix()
        .ok_or_else(|| decode("archive kind must be Pdf or Source"))?;
    let expected = format!(
        "{}/arXiv_{}_{:04}_{:03}.tar",
        prefix, prefix, archive.month, archive.sequence
    );
    if archive.sequence == 0 || archive.filename != expected {
        return Err(decode(
            "archive filename does not match kind, month and sequence",
        ));
    }
    if archive.checksum.len() != 32
        || !archive
            .checksum
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(decode("invalid archive MD5 checksum"));
    }
    Ok(())
}

pub(super) fn validate_archive_length(archive: &ArxivArchive, length: Option<u64>) -> Result<()> {
    if length.is_some_and(|length| length != archive.size) {
        return Err(decode("archive length differs from manifest metadata"));
    }
    Ok(())
}

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

/// Use the bulk archive spelling as the join key (legacy IDs omit the slash).
pub(super) fn metadata_id(id: &str) -> Result<String> {
    let id = id.strip_prefix("oai:arXiv.org:").unwrap_or(id);
    if !id.is_ascii() {
        return Err(invalid("expected an ASCII arXiv article ID"));
    }
    let key = id.replace('/', "");
    let valid = if key.as_bytes().first().is_some_and(u8::is_ascii_digit) {
        let Some((month, number)) = key.split_once('.') else {
            return Err(invalid("expected an arXiv article ID"));
        };
        month.len() == 4
            && month.bytes().all(|b| b.is_ascii_digit())
            && matches!(number.len(), 4 | 5)
            && number.bytes().all(|b| b.is_ascii_digit())
    } else {
        key.len() > 7 && {
            let (category, number) = key.split_at(key.len().saturating_sub(7));
            category
                .bytes()
                .all(|b| b.is_ascii_alphabetic() || b == b'-' || b == b'.')
                && number.bytes().all(|b| b.is_ascii_digit())
        }
    };
    if !valid || !id.is_ascii() || id.matches('/').count() > 1 {
        return Err(invalid("expected an unversioned arXiv article ID"));
    }
    Ok(key)
}

pub(super) fn pdf_id(name: &[u8]) -> Result<String> {
    let path = std::str::from_utf8(name).map_err(|_| decode("PDF entry name is not UTF-8"))?;
    let name = path.rsplit('/').next().unwrap_or(path);
    let stem = name
        .strip_suffix(".pdf")
        .ok_or_else(|| decode("expected a PDF entry"))?;
    let stem = match stem.rsplit_once('v') {
        Some((id, version))
            if !version.is_empty() && version.bytes().all(|b| b.is_ascii_digit()) =>
        {
            id
        }
        _ => stem,
    };
    // Some mirrors retain the legacy category directory instead of flattening it.
    let legacy;
    let stem = if stem.len() == 7 && stem.bytes().all(|b| b.is_ascii_digit()) {
        let category = path.rsplit('/').nth(1).unwrap_or("");
        legacy = format!("{category}/{stem}");
        legacy.as_str()
    } else {
        stem
    };
    metadata_id(stem).map_err(|_| decode("PDF entry has an invalid arXiv ID"))
}

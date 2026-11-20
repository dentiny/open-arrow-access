use arrow_array::Array;
use arrow_schema::DataType;
use open_arrow_access_core::Result;

use super::{ArxivArchive, ArxivKind, error};

pub(super) fn validate_unique_metadata_id(previous: Option<u64>) -> Result<()> {
    if previous.is_some() {
        return Err(error::duplicate_metadata_id());
    }
    Ok(())
}

pub(super) fn validate_pdf_kind(kind: ArxivKind) -> Result<()> {
    if kind != ArxivKind::Pdf {
        return Err(error::pdf_kind_required());
    }
    Ok(())
}

pub(super) fn validate_metadata_id_column(column: &dyn Array) -> Result<()> {
    if !matches!(
        column.data_type(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        return Err(error::metadata_ids_not_strings());
    }
    if column.null_count() != 0 {
        return Err(error::null_metadata_id());
    }
    Ok(())
}

pub(super) fn validate_unique_archive(inserted: bool) -> Result<()> {
    if !inserted {
        return Err(error::duplicate_archive());
    }
    Ok(())
}

pub(super) fn validate_month(month: u16) -> Result<()> {
    if month > 9999 {
        return Err(error::invalid_month_encoding());
    }
    if !(1..=12).contains(&(month % 100)) {
        return Err(error::invalid_month_number());
    }
    Ok(())
}

pub(super) fn validate_archive(archive: &ArxivArchive) -> Result<()> {
    validate_month(archive.month).map_err(|_| error::invalid_archive_month())?;
    let prefix = archive
        .kind
        .prefix()
        .ok_or_else(error::invalid_archive_kind)?;
    let expected = format!(
        "{}/arXiv_{}_{:04}_{:03}.tar",
        prefix, prefix, archive.month, archive.sequence
    );
    if archive.sequence == 0 || archive.filename != expected {
        return Err(error::invalid_archive_filename());
    }
    if archive.checksum.len() != 32
        || !archive
            .checksum
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(error::invalid_archive_checksum());
    }
    Ok(())
}

pub(super) fn validate_archive_length(archive: &ArxivArchive, length: Option<u64>) -> Result<()> {
    // Upstream manifests can report the sum of member payload sizes, excluding
    // TAR headers, alignment padding and end blocks. It is a lower bound, not
    // the object's exact byte length. TAR readers validate the actual framing.
    if length.is_some_and(|length| length < archive.size) {
        return Err(error::archive_length_mismatch());
    }
    Ok(())
}

/// Modern article download ID with an optional positive version suffix.
pub(super) fn validate_article_id(id: &str) -> Result<()> {
    let unversioned = if let Some((base, version)) = id.rsplit_once('v')
        && !version.is_empty()
        && version.bytes().all(|b| b.is_ascii_digit())
    {
        if version.starts_with('0') {
            return Err(error::invalid("invalid arXiv article version"));
        }
        base
    } else {
        id
    };
    let Some((month, number)) = unversioned.split_once('.') else {
        return Err(error::invalid("expected modern arXiv article ID"));
    };
    if month.len() != 4
        || !month.bytes().all(|b| b.is_ascii_digit())
        || !matches!(number.len(), 4 | 5)
        || !number.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(error::invalid("expected modern arXiv article ID"));
    }
    Ok(())
}

pub(super) fn validate_metadata_id(id: &str) -> Result<()> {
    if !id.is_ascii() {
        return Err(error::non_ascii_metadata_id());
    }
    let valid = if id.as_bytes().first().is_some_and(u8::is_ascii_digit) {
        let Some((month, number)) = id.split_once('.') else {
            return Err(error::invalid_metadata_id());
        };
        month.len() == 4
            && month.bytes().all(|b| b.is_ascii_digit())
            && matches!(number.len(), 4 | 5)
            && number.bytes().all(|b| b.is_ascii_digit())
    } else {
        let (category, number) = match id.split_once('/') {
            Some(parts) => parts,
            None if id.len() > 7 => id.split_at(id.len() - 7),
            None => return Err(error::invalid_unversioned_metadata_id()),
        };
        !category.is_empty()
            && category
                .bytes()
                .all(|b| b.is_ascii_alphabetic() || b == b'-' || b == b'.')
            && number.len() == 7
            && number.bytes().all(|b| b.is_ascii_digit())
    };
    if !valid {
        return Err(error::invalid_unversioned_metadata_id());
    }
    Ok(())
}

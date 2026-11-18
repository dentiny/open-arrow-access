use std::collections::HashMap;

use arrow_array::{Array, ArrayRef, LargeBinaryArray, RecordBatch, cast::AsArray};
use arrow_schema::DataType;
use open_arrow_access_core::Result;

use super::{ArxivArchive, ArxivKind, error};

pub(super) fn metadata_id_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef> {
    let column = batch
        .column_by_name(name)
        .ok_or_else(error::missing_metadata_id_column)?;
    validate_metadata_id_column(column.as_ref())?;
    Ok(column)
}

pub(super) fn validate_unique_metadata_id(previous: Option<u64>) -> Result<()> {
    if previous.is_some() {
        return Err(error::duplicate_metadata_id());
    }
    Ok(())
}

pub(super) fn pdf_entry_names(batch: &RecordBatch) -> Result<&LargeBinaryArray> {
    Ok(batch
        .column_by_name("entry_name")
        .ok_or_else(error::missing_pdf_entry_column)?
        .as_binary::<i64>())
}

pub(super) fn pdf_metadata_row(name: Option<&[u8]>, ids: &HashMap<String, u64>) -> Result<u64> {
    let id = pdf_id(name.ok_or_else(error::null_pdf_entry_name)?)?;
    ids.get(&id)
        .copied()
        .ok_or_else(error::missing_pdf_metadata)
}

pub(super) fn validate_pdf_kind(kind: ArxivKind) -> Result<()> {
    if kind != ArxivKind::Pdf {
        return Err(error::pdf_kind_required());
    }
    Ok(())
}

fn validate_metadata_id_column(column: &dyn Array) -> Result<()> {
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

pub(super) fn manifest_root(kind: ArxivKind) -> Result<&'static str> {
    kind.root().ok_or_else(error::invalid_manifest_kind)
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
    if length.is_some_and(|length| length != archive.size) {
        return Err(error::archive_length_mismatch());
    }
    Ok(())
}

/// Use the bulk archive spelling as the join key (legacy IDs omit the slash).
pub(super) fn metadata_id(id: &str) -> Result<String> {
    let id = id.strip_prefix("oai:arXiv.org:").unwrap_or(id);
    if !id.is_ascii() {
        return Err(error::non_ascii_metadata_id());
    }
    let key = id.replace('/', "");
    let valid = if key.as_bytes().first().is_some_and(u8::is_ascii_digit) {
        let Some((month, number)) = key.split_once('.') else {
            return Err(error::invalid_metadata_id());
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
    if !valid || id.matches('/').count() > 1 {
        return Err(error::invalid_unversioned_metadata_id());
    }
    Ok(key)
}

pub(super) fn pdf_id(name: &[u8]) -> Result<String> {
    let path = std::str::from_utf8(name).map_err(|_| error::invalid_pdf_entry_encoding())?;
    let name = path.rsplit('/').next().unwrap_or(path);
    let stem = name
        .strip_suffix(".pdf")
        .ok_or_else(|| error::invalid_pdf_extension())?;
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
    metadata_id(stem).map_err(|_| error::invalid_pdf_id())
}

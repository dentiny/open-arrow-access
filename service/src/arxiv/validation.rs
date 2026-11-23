use arrow_array::{Array, RecordBatch, cast::AsArray};
use arrow_schema::DataType;
use open_arrow_access_core::Result;

use super::{ArxivArchive, ArxivContentFormat, ArxivKind, ArxivMetadataIndex, error, id};

pub(super) fn validate_oai_metadata(batch: &RecordBatch) -> Result<()> {
    if batch.schema().as_ref() != crate::arxiv_metadata::schema().arrow_schema().as_ref() {
        return Err(error::invalid_oai_metadata_schema());
    }
    let header = batch["oai:header"].as_struct();
    if header.null_count() != 0 {
        return Err(error::null_metadata_id());
    }
    validate_metadata_id_column(header["oai:identifier"].as_ref())
}

pub(super) fn validate_metadata_record_count(count: usize) -> Result<()> {
    if count != 1 {
        return Err(error::invalid_metadata_response());
    }
    Ok(())
}

pub(super) fn validate_fetched_metadata(batch: &RecordBatch, article: &str) -> Result<()> {
    validate_metadata_record_count(batch.num_rows())?;
    validate_oai_metadata(batch)?;
    let header = batch["oai:header"].as_struct();
    let identifier = header["oai:identifier"].as_string::<i32>().value(0);
    if id::metadata_id(identifier)? != article {
        return Err(error::invalid_metadata_response());
    }
    let status = header["@status"].as_string::<i32>();
    if !status.is_null(0) && status.value(0) == "deleted" {
        return Ok(());
    }
    let metadata = batch["oai:metadata"].as_struct();
    let raw = metadata["arxiv:arXivRaw"].as_struct();
    let raw_id = raw["arxiv:id"].as_string::<i32>();
    if metadata.is_null(0)
        || raw.is_null(0)
        || raw_id.is_null(0)
        || id::metadata_id(raw_id.value(0))? != article
    {
        return Err(error::invalid_metadata_response());
    }
    Ok(())
}

pub(super) fn validate_content_schema(metadata: &ArxivMetadataIndex) -> Result<()> {
    if metadata.rows.fields() != crate::arxiv_metadata::schema().arrow_schema().fields() {
        return Err(error::invalid_content_metadata_schema());
    }
    Ok(())
}

pub(super) fn validate_content_metadata_match(row: Option<u64>, id: &str) -> Result<u64> {
    row.ok_or_else(|| error::missing_content_metadata(id))
}

/// TAR headers occupy one fixed 512-byte block.
pub(super) const TAR_HEADER_SIZE: usize = 512;

pub(super) fn validate_unique_metadata_id(previous: Option<u64>) -> Result<()> {
    if previous.is_some() {
        return Err(error::duplicate_metadata_id());
    }
    Ok(())
}

pub(super) fn validate_shard(shard: &str, month: Option<u16>) -> Result<()> {
    if month.is_some()
        || !shard.ends_with(".tar")
        || shard.starts_with('/')
        || shard.contains(['\\', ':', '?', '#'])
        || shard.split('/').any(|part| matches!(part, "" | "." | ".."))
    {
        return Err(error::invalid_shard());
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

pub(super) fn validate_content_kind(format: ArxivContentFormat, kind: ArxivKind) -> Result<()> {
    match (format, kind) {
        (ArxivContentFormat::Pdf, ArxivKind::Pdf)
        | (ArxivContentFormat::Latex | ArxivContentFormat::Xml, ArxivKind::Source) => Ok(()),
        _ => Err(error::invalid(
            "PDF content requires kind=pdf; LaTeX/XML content requires kind=source",
        )),
    }
}

pub(super) fn is_tex(name: &[u8]) -> Result<bool> {
    if !name.to_ascii_lowercase().ends_with(b".tex") {
        return Ok(false);
    }
    let name =
        std::str::from_utf8(name).map_err(|_| error::decode("LaTeX member name is not UTF-8"))?;
    let parts = name.split('/').collect::<Vec<_>>();
    if name.starts_with('/') || parts.contains(&"..") || name.contains('\\') {
        return Err(error::decode("unsafe source member name"));
    }
    Ok(!parts.iter().any(|part| part.eq_ignore_ascii_case("anc"))
        && name.to_ascii_lowercase().ends_with(".tex"))
}
pub(super) fn is_tar(data: &[u8]) -> bool {
    if data.len() < TAR_HEADER_SIZE {
        return false;
    }
    if &data[257..262] == b"ustar" {
        return true;
    }
    let checksum = std::str::from_utf8(&data[148..156])
        .ok()
        .and_then(|s| u64::from_str_radix(s.trim_matches(['\0', ' ']), 8).ok());
    let sum: u64 = data[..TAR_HEADER_SIZE]
        .iter()
        .enumerate()
        .map(|(i, &v)| {
            if (148..156).contains(&i) {
                32
            } else {
                u64::from(v)
            }
        })
        .sum();
    checksum == Some(sum)
}

pub(super) fn content_article_id(name: &[u8], format: ArxivContentFormat) -> Result<String> {
    if format == ArxivContentFormat::Pdf {
        return id::pdf_id(name);
    }
    let path =
        std::str::from_utf8(name).map_err(|_| error::decode("article filename is not UTF-8"))?;
    let mut stem = path.to_owned();
    match format {
        ArxivContentFormat::Latex => {
            // Source shards also contain standalone PDF/PostScript submissions.
            // Normalize their article identity before expanding source content.
            for suffix in [".gz", ".tar", ".tex", ".pdf", ".ps"] {
                if let Some(value) = stem.strip_suffix(suffix) {
                    stem = value.to_owned();
                }
            }
        }
        ArxivContentFormat::Xml => {
            stem = path
                .strip_suffix(".xml")
                .ok_or_else(|| error::decode("XML shard member must end in .xml"))?
                .to_owned();
        }
        ArxivContentFormat::Pdf => unreachable!(),
    }
    id::pdf_id(format!("{stem}.pdf").as_bytes())
}

//! Parse and normalize article IDs used to join metadata to bulk PDFs.
use open_arrow_access_core::Result;

use super::{error, validation};

/// Use the bulk archive spelling as the join key (legacy IDs omit the slash).
pub(super) fn metadata_id(id: &str) -> Result<String> {
    let id = id.strip_prefix("oai:arXiv.org:").unwrap_or(id);
    validation::validate_metadata_id(id)?;
    Ok(id.replace('/', ""))
}

/// Restore the slash required by OAI for a legacy bulk identifier.
pub(super) fn oai_identifier(article: &str) -> Result<String> {
    let article = metadata_id(article)?;
    let canonical = if article.as_bytes()[0].is_ascii_digit() {
        article
    } else {
        let (category, number) = article.split_at(article.len() - 7);
        format!("{category}/{number}")
    };
    Ok(format!("oai:arXiv.org:{canonical}"))
}

pub(super) fn pdf_id(name: &[u8]) -> Result<String> {
    let path = std::str::from_utf8(name).map_err(|_| error::invalid_pdf_entry_encoding())?;
    let name = path.rsplit('/').next().unwrap_or(path);
    let stem = name
        .strip_suffix(".pdf")
        .ok_or_else(error::invalid_pdf_extension)?;
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

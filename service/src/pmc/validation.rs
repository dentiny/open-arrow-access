use super::decode;
use open_arrow_access_core::{Error, ErrorKind, Result};

pub(super) fn validate_pmcid(pmcid: &str, path: &str) -> Result<()> {
    let value = pmcid.trim();
    let digits = if value
        .get(..3)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("PMC"))
    {
        &value[3..]
    } else {
        value
    };
    if digits.is_empty()
        || !digits.bytes().all(|b| b.is_ascii_digit())
        || digits.bytes().all(|b| b == b'0')
    {
        return Err(decode(format!("{path}: invalid PMCID")));
    }
    Ok(())
}
pub(super) fn validate_article_version(version: Option<u32>, path: &str) -> Result<()> {
    if version == Some(0) {
        return Err(decode(format!("{path}: article version must be positive")));
    }
    Ok(())
}

pub(super) fn validate_content_length(length: Option<u64>, limit: usize, path: &str) -> Result<()> {
    if length.is_some_and(|size| size > limit as u64) {
        return Err(object_limit(path, limit));
    }
    Ok(())
}

pub(super) fn validate_chunk(
    bytes: &[u8],
    remaining: usize,
    limit: usize,
    path: &str,
) -> Result<()> {
    if bytes.is_empty() {
        return Err(Error::new(
            ErrorKind::Io,
            "read_pmc",
            format!("{path}: empty byte chunk"),
        ));
    }
    if bytes.len() > remaining {
        return Err(object_limit(path, limit));
    }
    Ok(())
}

fn object_limit(path: &str, limit: usize) -> Error {
    decode(format!(
        "{path}: exceeds configured {limit}-byte object limit"
    ))
}

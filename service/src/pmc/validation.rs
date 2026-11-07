use super::{decode, invalid};
use open_arrow_access_core::{Error, ErrorKind, ObjectRef, Result};

pub(super) fn validate_metadata_objects(objects: &[ObjectRef]) -> Result<()> {
    if objects.iter().any(|object| !valid_path(object.locator())) {
        return Err(invalid(
            "metadata objects must be bucket-relative paths without dot components",
        ));
    }
    Ok(())
}

pub(super) fn validate_listing_support(supported: bool) -> Result<()> {
    if !supported {
        return Err(Error::new(
            ErrorKind::Unsupported,
            "open_pmc",
            "byte accessor does not support listing; use explicit metadata files",
        ));
    }
    Ok(())
}

pub(super) fn validate_prefix(prefix: &str) -> Result<()> {
    let path = prefix.strip_suffix('/').unwrap_or(prefix);
    if prefix == "/" || (!path.is_empty() && !valid_path(path)) {
        return Err(invalid("expected a bucket-relative directory prefix"));
    }
    Ok(())
}

pub(super) fn validate_discovered_path<'a>(
    path: &'a str,
    prefix: &str,
    recursive: bool,
) -> Result<Option<&'a str>> {
    let relative = path
        .strip_prefix(prefix)
        .ok_or_else(|| invalid("PMC listing returned an object outside its prefix"))?;
    if relative.is_empty() || relative.ends_with('/') {
        return Ok(None);
    }
    if !valid_path(relative) || (!recursive && relative.contains('/')) {
        return Err(invalid("PMC listing returned an invalid object path"));
    }
    Ok(Some(relative))
}

pub(super) fn validate_pmcid_digits(digits: &str, path: &str) -> Result<()> {
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

pub(super) fn validate_pdf_url(uri: &str) -> Result<String> {
    let invalid = || decode(format!("invalid PMC PDF URL: {uri}"));
    let path = uri
        .strip_prefix("s3://pmc-oa-opendata/")
        .ok_or_else(invalid)?;
    let path = path.split(['?', '#']).next().unwrap_or_default();
    // Decode exactly once; '+' is a literal filename character.
    let path = percent_encoding::percent_decode_str(path)
        .decode_utf8()
        .map_err(|_| invalid())?;
    if !valid_path(&path) {
        return Err(invalid());
    }
    Ok(path.into_owned())
}

pub(super) fn validate_pdf(bytes: &[u8], path: &str) -> Result<()> {
    // Header validation only, not a PDF parser or completeness check.
    if bytes.len() < 9
        || !bytes.starts_with(b"%PDF-")
        || !bytes[5].is_ascii_digit()
        || bytes[6] != b'.'
        || !bytes[7].is_ascii_digit()
        || !matches!(bytes[8], b'\r' | b'\n')
    {
        return Err(decode(format!("{path}: invalid PDF header")));
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

fn valid_path(path: &str) -> bool {
    !path.is_empty()
        && path.trim() == path
        && !path.contains(['\\', '?', '#'])
        && !path.contains("://")
        && !path.chars().any(char::is_control)
        && !path.split('/').any(|part| matches!(part, "" | "." | ".."))
}

use super::{decode, manifest::Manifest};
use open_arrow_access_core::{Error, ErrorKind, Result};
use std::collections::HashSet;

pub(super) fn validate_manifest_locator(path: &str) -> Result<()> {
    if !valid_path(path) || !(path == "manifest.json" || path.ends_with("/manifest.json")) {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "open_pmc_inventory",
            "expected an explicit snapshot manifest object",
        ));
    }
    Ok(())
}
pub(super) fn validate_manifest(manifest: &Manifest) -> Result<()> {
    if manifest.source_bucket != "pmc-oa-opendata"
        || manifest.destination_bucket != "arn:aws:s3:::pmc-oa-opendata"
    {
        return Err(decode(
            "expected a PMC metadata inventory in the pmc-oa-opendata bucket",
        ));
    }
    if manifest.file_format != "CSV" {
        return Err(Error::new(
            ErrorKind::Unsupported,
            "open_pmc_inventory",
            "only CSV inventory manifests are supported",
        ));
    }
    if manifest.version.is_empty()
        || manifest.creation_timestamp.is_empty()
        || !manifest
            .creation_timestamp
            .bytes()
            .all(|b| b.is_ascii_digit())
    {
        return Err(decode("missing inventory version or creation timestamp"));
    }
    validate_fields(&manifest.file_schema)?;
    for file in &manifest.files {
        if !valid_path(&file.key)
            || !file.key.ends_with(".csv.gz")
            || file.size == 0
            || file.checksum.len() != 32
            || !file.checksum.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(decode("invalid inventory shard entry"));
        }
    }
    Ok(())
}
pub(super) fn validate_fields(schema: &str) -> Result<Vec<String>> {
    let fields: Vec<String> = schema.split(',').map(|s| s.trim().to_owned()).collect();
    let mut seen = HashSet::new();
    if fields
        .iter()
        .any(|field| field.is_empty() || field == "metadata_key" || !seen.insert(field.as_str()))
        || ["Bucket", "Key", "LastModifiedDate", "ETag"]
            .iter()
            .any(|required| !seen.contains(required))
    {
        return Err(decode(
            "invalid inventory fileSchema: required PMC fields must be unique",
        ));
    }
    Ok(fields)
}
pub(super) fn validate_row(bucket: Option<&str>, key: Option<&str>) -> Result<String> {
    if bucket != Some("pmc-oa-opendata") {
        return Err(decode("inventory row has a missing or unexpected Bucket"));
    }
    let key = key.ok_or_else(|| decode("inventory row has no Key"))?;
    let bytes = key.as_bytes();
    for (index, value) in bytes.iter().enumerate() {
        if *value == b'%'
            && !(bytes.get(index + 1).is_some_and(u8::is_ascii_hexdigit)
                && bytes.get(index + 2).is_some_and(u8::is_ascii_hexdigit))
        {
            return Err(decode("invalid percent encoding in inventory Key"));
        }
    }
    let decoded = percent_encoding::percent_decode_str(key)
        .decode_utf8()
        .map_err(|error| decode("invalid UTF-8 inventory Key").with_source(error))?;
    if !valid_path(&decoded) || !decoded.starts_with("metadata/") || !decoded.ends_with(".json") {
        return Err(decode(
            "inventory Key is not a canonical metadata JSON object",
        ));
    }
    Ok(decoded.into_owned())
}
pub(super) fn validate_manifest_length(length: Option<u64>, limit: usize) -> Result<()> {
    if length.is_some_and(|length| length > limit as u64) {
        return Err(decode("manifest exceeds configured byte limit"));
    }
    Ok(())
}
pub(super) fn validate_manifest_chunk(chunk: &[u8], remaining: usize) -> Result<()> {
    if chunk.is_empty() {
        return Err(Error::new(
            ErrorKind::Io,
            "read_pmc_inventory",
            "empty manifest byte chunk",
        ));
    }
    if chunk.len() > remaining {
        return Err(decode("manifest exceeds configured byte limit"));
    }
    Ok(())
}
fn valid_path(path: &str) -> bool {
    !path.is_empty()
        && path.trim() == path
        && !path.contains(['\\', '?', '#'])
        && !path.contains("://")
        && !path.chars().any(char::is_control)
        && !path.split('/').any(|p| matches!(p, "" | "." | ".."))
}

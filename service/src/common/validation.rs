//! Input constraints and byte-stream invariants for shared provider utilities.
use super::{MetadataLink, decode, invalid};
use open_arrow_access_core::Result;
use url::Url;

pub(super) fn download_url(url: &Url) -> Result<()> {
    if !crate::http_client::validation::is_http_resource_url(url) {
        return Err(decode("expected credential-free HTTP(S) download URL"));
    }
    Ok(())
}

pub(super) fn identifier(value: &str) -> Result<()> {
    if value.is_empty() || matches!(value, "." | "..") {
        return Err(invalid("empty or dot identifier"));
    }
    Ok(())
}

pub(crate) fn filename_component(value: &str) -> Result<()> {
    if !crate::filename::valid_filename_component(value) {
        return Err(invalid("invalid filename component"));
    }
    Ok(())
}

pub(super) fn provider_path(path: &str) -> Result<()> {
    if path.starts_with('/')
        || path.contains(['?', '#', '\\'])
        || path.split('/').any(|s| matches!(s, "." | ".."))
    {
        return Err(invalid("expected relative provider path"));
    }
    Ok(())
}

pub(super) fn same_origin(origin: &Url, url: &Url) -> Result<()> {
    if url.origin() != origin.origin() {
        return Err(invalid("API continuation changed origin"));
    }
    Ok(())
}

pub(super) fn redirect(current: &Url, next: &Url) -> Result<()> {
    if current.scheme() == "https" && next.scheme() != "https" {
        return Err(decode("HTTPS downgrade redirect"));
    }
    Ok(())
}

pub(super) fn validate_downloads(files: &[MetadataLink]) -> Result<()> {
    for file in files {
        super::resolve_download_url(&file.url)?;
    }
    Ok(())
}

/// Recognize a complete SHA-1 Git object ID, without accepting branch names or short IDs.
pub(crate) fn is_commit_sha(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

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
#[allow(
    dead_code,
    reason = "Shared utility may be unused by the enabled providers."
)]
pub(crate) fn is_commit_sha(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[allow(
    dead_code,
    reason = "Shared utility may be unused by the enabled providers."
)]
pub(crate) fn reject_reserved_parameters(
    parameters: &[(&str, &str)],
    reserved: &[&str],
    message: &str,
) -> Result<()> {
    if parameters.iter().any(|(name, _)| reserved.contains(name)) {
        return Err(invalid(message));
    }
    Ok(())
}

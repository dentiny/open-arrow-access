use open_arrow_access_core::Result;

use super::error::invalid_origin;
use url::Url;

pub(crate) fn validate_origin(value: &str, operation: &'static str) -> Result<Url> {
    let invalid = || invalid_origin(operation);
    let url = Url::parse(value).map_err(|_| invalid())?;
    if !is_http_endpoint(&url) || url.path() != "/" {
        return Err(invalid());
    }
    Ok(url)
}

/// Shared endpoint rules; callers decide whether a path is allowed.
pub(crate) fn is_http_endpoint(url: &Url) -> bool {
    matches!(url.scheme(), "http" | "https")
        && url.host_str().is_some()
        && url.username().is_empty()
        && url.password().is_none()
        && url.query().is_none()
        && url.fragment().is_none()
}

/// Decode metadata through the shared error context.
pub(crate) fn json<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    operation: &'static str,
) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|error| super::json_error(operation, error))
}

/// Preserve source order while rejecting invalid logical namespaces once.
pub(super) fn order_files<T>(files: Vec<(String, T)>) -> Result<indexmap::IndexMap<String, T>> {
    let mut ordered = indexmap::IndexMap::with_capacity(files.len());
    for (name, file) in files {
        if name.is_empty() || ordered.insert(name, file).is_some() {
            return Err(super::error::empty_or_dup_filename());
        }
    }
    Ok(ordered)
}

use open_arrow_access_core::Result;

use super::error::invalid_origin;
use url::Url;

pub(crate) fn validate_origin(url: &Url, operation: &'static str) -> Result<()> {
    if !is_http_endpoint(url) || url.path() != "/" {
        return Err(invalid_origin(operation));
    }
    Ok(())
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

pub(super) fn validate_file_names<'a>(names: impl IntoIterator<Item = &'a str>) -> Result<()> {
    let mut seen = std::collections::HashSet::new();
    for name in names {
        if name.is_empty() || !seen.insert(name) {
            return Err(super::error::empty_or_dup_filename());
        }
    }
    Ok(())
}

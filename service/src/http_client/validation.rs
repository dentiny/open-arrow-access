use open_arrow_access_core::Result;

use super::error::invalid_origin;
use url::Url;

pub(super) fn validate_origin(value: &str, operation: &'static str) -> Result<Url> {
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

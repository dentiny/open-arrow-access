use open_arrow_access_core::Result;

use super::error::invalid_origin;
use url::Url;

pub(super) fn validate_origin(value: &str, operation: &'static str) -> Result<Url> {
    let invalid = || invalid_origin(operation);
    let url = Url::parse(value).map_err(|_| invalid())?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        return Err(invalid());
    }
    Ok(url)
}

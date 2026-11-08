use open_arrow_access_core::Result;
use url::Url;

use super::invalid;

pub(super) fn validate_endpoint(endpoint: &Url) -> Result<()> {
    if !matches!(endpoint.scheme(), "http" | "https")
        || endpoint.host_str().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
        || endpoint.path() != "/"
    {
        return Err(invalid("expected an HTTP(S) origin without credentials"));
    }
    Ok(())
}

use open_arrow_access_core::Result;
use url::Url;

use super::error::invalid_url;

pub(super) fn parse_url(value: &str) -> Result<Url> {
    let url = Url::parse(value).map_err(|error| invalid_url().with_source(error))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || value.trim() != value
        || value.chars().any(char::is_control)
        || value.contains(['%', '\\'])
        || value.split('/').any(|part| matches!(part, "." | ".."))
        || url
            .path()
            .strip_prefix('/')
            .unwrap_or(url.path())
            .contains("//")
    {
        return Err(invalid_url());
    }
    Ok(url)
}

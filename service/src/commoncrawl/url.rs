use open_arrow_access_core::Result;
use url::Url;

use super::error::invalid_url;
use crate::http_client::validation::is_http_endpoint;

pub(super) fn parse_url(value: &str) -> Result<Url> {
    let url = Url::parse(value).map_err(|error| invalid_url().with_source(error))?;
    if !is_http_endpoint(&url)
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

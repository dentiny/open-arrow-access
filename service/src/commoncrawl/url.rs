use open_arrow_access_core::{Error, ErrorKind, Result};
use url::Url;

use super::error::invalid_url;
use crate::http_client::validation::is_http_endpoint;

pub(super) fn parse_origin(value: &str, operation: &'static str) -> Result<Url> {
    let url = parse_url(value)?;
    if url.path() != "/" {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            operation,
            "expected an HTTP(S) origin without a path",
        ));
    }
    Ok(url)
}

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

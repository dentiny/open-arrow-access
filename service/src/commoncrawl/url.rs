use open_arrow_access_core::Result;
use url::Url;

use super::validation;

pub(super) fn parse_origin(value: &str, operation: &'static str) -> Result<Url> {
    let url = parse_url(value)?;
    validation::validate_origin_path(&url, operation)?;
    Ok(url)
}

pub(super) fn parse_url(value: &str) -> Result<Url> {
    let url = Url::parse(value).map_err(super::error::url_parse)?;
    validation::validate_url(value, &url)?;
    Ok(url)
}

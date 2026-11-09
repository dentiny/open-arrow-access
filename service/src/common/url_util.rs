//! URL parsing, construction and component encoding shared by providers.
use super::{error, validation};
use open_arrow_access_core::Result;
use url::Url;

/// Parse an absolute download URL without altering its signed query string.
pub fn download_url(value: &str) -> Result<Url> {
    let url = Url::parse(value).map_err(error::download_url)?;
    validation::download_url(&url)?;
    Ok(url)
}

/// Encode one provider path component, including slashes in opaque IDs/tokens.
pub fn segment(value: &str) -> Result<String> {
    validation::identifier(value)?;
    let mut url = Url::parse("https://encode.invalid/").unwrap();
    url.path_segments_mut().unwrap().push(value);
    Ok(url.path()[1..].to_owned())
}

/// Validate and encode one logical filename using the existing provider filename rules.
pub fn filename(value: &str) -> Result<String> {
    validation::filename_component(value)?;
    segment(value)
}

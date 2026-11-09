//! URL parsing, construction and component encoding shared by providers.
use super::{error, validation};
use http::HeaderMap;
use open_arrow_access_core::Result;
use url::Url;

/// Parse an absolute download URL without altering its signed query string.
pub fn resolve_download_url(value: &str) -> Result<Url> {
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

/// Encode slash-separated provider paths, rejecting empty and dot components.
pub fn encode_path(value: &str) -> Result<String> {
    value
        .split('/')
        .map(segment)
        .collect::<Result<Vec<_>>>()
        .map(|parts| parts.join("/"))
}

/// Validate and encode one logical filename component.
pub fn filename(value: &str) -> Result<String> {
    super::validate_filename(value)?;
    segment(value)
}

pub(super) fn provider_url(origin: &Url, path: &str, query: &[(&str, &str)]) -> Result<Url> {
    validation::provider_path(path)?;
    let mut url = origin.join(path).map_err(error::provider_path)?;
    validation::same_origin(origin, &url)?;
    if !query.is_empty() {
        url.query_pairs_mut().extend_pairs(query.iter().copied());
    }
    Ok(url)
}

pub(super) fn redirect(url: &Url, headers: &HeaderMap) -> Result<Url> {
    let location = headers
        .get("location")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| error::decode("redirect lacks Location"))?;
    let next = resolve_download_url(url.join(location).map_err(error::redirect)?.as_str())?;
    validation::redirect(url, &next)?;
    Ok(next)
}

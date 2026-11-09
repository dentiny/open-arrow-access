use super::{decode, invalid};
use open_arrow_access_core::Result;
use serde_json::Value;
use url::Url;

/// Validate an absolute download URL without altering its signed query string.
pub(crate) fn download_url(value: &str) -> Result<Url> {
    let url = Url::parse(value).map_err(|_| decode("invalid download URL"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(decode("expected credential-free HTTP(S) download URL"));
    }
    Ok(url)
}

/// Encode one provider path component, including slashes in opaque IDs/tokens.
pub fn segment(value: &str) -> Result<String> {
    if value.is_empty() || matches!(value, "." | "..") {
        return Err(invalid("empty or dot identifier"));
    }
    let mut url = Url::parse("https://encode.invalid/").unwrap();
    url.path_segments_mut().unwrap().push(value);
    Ok(url.path()[1..].to_owned())
}

/// Required JSON field validation shared by provider-specific schema mappings.
pub fn text<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| decode(&format!("missing or invalid {field}")))
}
/// Required JSON array validation; missing and null are not silently treated as empty.
pub fn array<'a>(value: &'a Value, field: &str) -> Result<&'a Vec<Value>> {
    value
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| decode(&format!("missing or invalid {field}")))
}

/// Validate and encode one logical filename using the existing provider filename rules.
pub fn filename(value: &str) -> Result<String> {
    if !crate::filename::valid_filename_component(value) {
        return Err(invalid("invalid filename component"));
    }
    segment(value)
}

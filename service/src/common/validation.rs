use super::error;
use super::{decode, invalid};
use http::{HeaderMap, HeaderValue, header::HeaderName};
use open_arrow_access_core::Result;
use serde_json::Value;
use url::Url;

/// Validate an absolute download URL without altering its signed query string.
pub fn download_url(value: &str) -> Result<Url> {
    let url = Url::parse(value).map_err(error::download_url)?;
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
        .ok_or_else(|| error::missing_field(field))
}
/// Required JSON array validation; missing and null are not silently treated as empty.
pub fn array<'a>(value: &'a Value, field: &str) -> Result<&'a Vec<Value>> {
    value
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| error::missing_field(field))
}

/// Validate and encode one logical filename using the existing provider filename rules.
pub fn filename(value: &str) -> Result<String> {
    if !crate::filename::valid_filename_component(value) {
        return Err(invalid("invalid filename component"));
    }
    segment(value)
}

pub(super) fn header(name: &str, value: &str) -> Result<(HeaderName, HeaderValue)> {
    let name = HeaderName::from_bytes(name.as_bytes()).map_err(error::header_name)?;
    let mut value = HeaderValue::from_str(value).map_err(error::header_value)?;
    value.set_sensitive(true);
    Ok((name, value))
}
pub(super) fn provider_url(origin: &Url, path: &str, query: &[(&str, &str)]) -> Result<Url> {
    if path.starts_with('/')
        || path.contains(['?', '#', '\\'])
        || path.split('/').any(|s| matches!(s, "." | ".."))
    {
        return Err(invalid("expected relative provider path"));
    }
    let mut url = origin.join(path).map_err(error::provider_path)?;
    same_origin(origin, &url)?;
    if !query.is_empty() {
        url.query_pairs_mut().extend_pairs(query.iter().copied());
    }
    Ok(url)
}
pub(super) fn same_origin(origin: &Url, url: &Url) -> Result<()> {
    if url.origin() != origin.origin() {
        return Err(invalid("API continuation changed origin"));
    }
    Ok(())
}
pub(super) fn redirect(url: &Url, headers: &HeaderMap) -> Result<Url> {
    let location = headers
        .get("location")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| decode("redirect lacks Location"))?;
    let next = download_url(url.join(location).map_err(error::redirect)?.as_str())?;
    if url.scheme() == "https" && next.scheme() != "https" {
        return Err(decode("HTTPS downgrade redirect"));
    }
    Ok(next)
}
pub(super) fn json(bytes: &[u8]) -> Result<Value> {
    serde_json::from_slice(bytes).map_err(error::json)
}
/// Validate body length for buffered metadata and streaming downloads alike.
pub(super) struct BodyLength {
    pub(super) expected: Option<u64>,
    read: u64,
}
impl BodyLength {
    pub(super) fn new(headers: &HeaderMap) -> Result<Self> {
        let expected = headers
            .get("content-length")
            .map(|value| {
                value
                    .to_str()
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok())
                    .ok_or_else(|| decode("invalid Content-Length"))
            })
            .transpose()?;
        Ok(Self { expected, read: 0 })
    }
    pub(super) fn push(&mut self, bytes: usize) -> Result<()> {
        self.read = self
            .read
            .checked_add(bytes as u64)
            .ok_or_else(|| decode("body length overflow"))?;
        if self.expected.is_some_and(|length| self.read > length) {
            return Err(decode("HTTP body exceeds Content-Length"));
        }
        Ok(())
    }
    pub(super) fn finish(&self) -> Result<()> {
        if self.expected.is_some_and(|length| self.read != length) {
            return Err(decode("truncated HTTP body"));
        }
        Ok(())
    }
}

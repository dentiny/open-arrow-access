//! Input constraints and byte-stream invariants for shared provider utilities.
use super::{decode, invalid};
use http::HeaderMap;
use open_arrow_access_core::Result;
use url::Url;

pub(super) fn download_url(url: &Url) -> Result<()> {
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(decode("expected credential-free HTTP(S) download URL"));
    }
    Ok(())
}

pub(super) fn identifier(value: &str) -> Result<()> {
    if value.is_empty() || matches!(value, "." | "..") {
        return Err(invalid("empty or dot identifier"));
    }
    Ok(())
}

pub(super) fn filename_component(value: &str) -> Result<()> {
    if !crate::filename::valid_filename_component(value) {
        return Err(invalid("invalid filename component"));
    }
    Ok(())
}

pub(super) fn provider_path(path: &str) -> Result<()> {
    if path.starts_with('/')
        || path.contains(['?', '#', '\\'])
        || path.split('/').any(|s| matches!(s, "." | ".."))
    {
        return Err(invalid("expected relative provider path"));
    }
    Ok(())
}

pub(super) fn same_origin(origin: &Url, url: &Url) -> Result<()> {
    if url.origin() != origin.origin() {
        return Err(invalid("API continuation changed origin"));
    }
    Ok(())
}

pub(super) fn redirect(current: &Url, next: &Url) -> Result<()> {
    if current.scheme() == "https" && next.scheme() != "https" {
        return Err(decode("HTTPS downgrade redirect"));
    }
    Ok(())
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

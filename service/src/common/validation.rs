//! Input constraints and byte-stream invariants for shared provider utilities.
use super::{decode, invalid};
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

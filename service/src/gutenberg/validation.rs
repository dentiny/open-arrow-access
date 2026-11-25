use super::error;
use open_arrow_access_core::Result;
use url::Url;

pub(super) fn mirror_uri(url: &Url, path: &str, uri: &str) -> Result<()> {
    if !matches!(url.scheme(), "http" | "https")
        || !matches!(url.host_str(), Some("www.gutenberg.org" | "gutenberg.org"))
        || url.port().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
        || path.split('/').any(|part| matches!(part, "." | ".."))
        || path.contains(['\\', '\0'])
    {
        return Err(error::invalid_mirror_uri(uri));
    }
    Ok(())
}

pub(super) fn mirror_ebook_id(id: &str, uri: &str) -> Result<()> {
    if id.is_empty() || !id.bytes().all(|b| b.is_ascii_digit()) {
        return Err(error::invalid_mirror_uri(uri));
    }
    Ok(())
}

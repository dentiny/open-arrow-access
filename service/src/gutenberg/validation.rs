use super::error;
use open_arrow_access_core::Result;
use url::Url;

pub(super) fn mirror_uri(url: &Url, uri: &str) -> Result<()> {
    if !matches!(url.scheme(), "http" | "https")
        || !matches!(url.host_str(), Some("www.gutenberg.org" | "gutenberg.org"))
        || url.port().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(error::invalid_mirror_uri(uri));
    }
    Ok(())
}

pub(super) fn mirror_path(path: &str, uri: &str) -> Result<()> {
    if path.split('/').any(|part| matches!(part, "." | "..")) || path.contains(['\\', '\0']) {
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

pub(super) fn required_field(name: &str, nullable: bool, text: Option<&str>) -> Result<()> {
    if !nullable && text.is_none_or(str::is_empty) {
        return Err(error::missing_field(name));
    }
    Ok(())
}

pub(super) fn ebook_id(id: &str, value: u64) -> Result<()> {
    if value == 0 || !id.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(error::invalid_ebook_id());
    }
    Ok(())
}

use super::error::invalid;
use open_arrow_access_core::Result;
use url::Url;

pub(super) fn validate_hosted_book(origin: &Url, link: &Url) -> Result<()> {
    if link.origin() != origin.origin() {
        return Err(invalid("expected an OAPEN-hosted content link"));
    }
    Ok(())
}

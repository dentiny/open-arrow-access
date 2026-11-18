use open_arrow_access_core::Result;

pub(super) fn validate_manifest_url(manifest_url: &url::Url) -> Result<()> {
    if manifest_url.path().ends_with('/') {
        return Err(super::error::invalid_manifest_url());
    }
    Ok(())
}

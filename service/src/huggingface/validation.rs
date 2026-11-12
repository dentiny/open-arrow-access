use open_arrow_access_core::Result;

use url::Url;

use super::{HuggingFaceDatasetRequest, HuggingFaceOptions, HuggingFaceRepository};
use crate::common::is_commit_sha;

pub(super) fn validate_repository(repository: &HuggingFaceRepository) -> Result<()> {
    validate_repository_id(&repository.repository)?;
    validate_revision(&repository.revision)
}

fn validate_repository_id(repository: &str) -> Result<()> {
    let parts: Vec<_> = repository.split('/').collect();
    if parts.len() != 2
        || parts.iter().any(|p| {
            p.is_empty()
                || *p == "."
                || *p == ".."
                || !p
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        })
    {
        return Err(super::error::invalid_repository());
    }
    Ok(())
}

fn validate_revision(revision: &str) -> Result<()> {
    if revision.is_empty()
        || revision.trim() != revision
        || revision.contains(['\0', '\\'])
        || matches!(revision, "." | "..")
    {
        return Err(super::error::invalid_revision());
    }
    Ok(())
}

pub(super) fn validate_options(options: &HuggingFaceOptions) -> Result<()> {
    endpoint(options.endpoint())?;
    if let Some(token) = &options.token
        && (token.is_empty() || http::HeaderValue::from_str(&format!("Bearer {token}")).is_err())
    {
        return Err(super::error::invalid_token());
    }
    Ok(())
}

pub(super) fn endpoint(value: &str) -> Result<Url> {
    let invalid = super::error::invalid_endpoint;
    let url = Url::parse(value).map_err(|_| invalid())?;
    if !crate::http_client::validation::is_http_endpoint(&url) {
        return Err(invalid());
    }
    Ok(url)
}

pub(super) fn validate_dataset_request(
    request: &HuggingFaceDatasetRequest,
    repository: &HuggingFaceRepository,
) -> Result<()> {
    validate_repository(repository)?;
    if request.split.starts_with("partial-") && !request.allow_partial {
        return Err(super::error::partial_export_requires_opt_in());
    }
    Ok(())
}

pub(super) fn validate_commit_sha(sha: &str) -> Result<()> {
    if !is_commit_sha(sha) {
        return Err(super::error::missing_commit_sha());
    }
    Ok(())
}

pub(super) fn validate_split(objects: &[open_arrow_access_core::ObjectRef]) -> Result<()> {
    if objects.is_empty() {
        return Err(super::error::empty_split());
    }
    Ok(())
}

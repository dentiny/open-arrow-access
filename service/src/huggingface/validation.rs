use open_arrow_access_core::{Error, ErrorKind, Result};

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
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "configure_huggingface",
            "expected a repository ID in owner/name form",
        ));
    }
    Ok(())
}

fn validate_revision(revision: &str) -> Result<()> {
    if revision.is_empty()
        || revision.trim() != revision
        || revision.contains(['\0', '\\'])
        || matches!(revision, "." | "..")
    {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "configure_huggingface",
            "invalid repository revision",
        ));
    }
    Ok(())
}

pub(super) fn validate_options(options: &HuggingFaceOptions) -> Result<()> {
    endpoint(options.endpoint())?;
    if let Some(token) = &options.token
        && (token.is_empty() || http::HeaderValue::from_str(&format!("Bearer {token}")).is_err())
    {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "configure_huggingface",
            "invalid Hub authentication token",
        ));
    }
    Ok(())
}

pub(super) fn endpoint(value: &str) -> Result<Url> {
    let invalid = || {
        Error::new(
            ErrorKind::InvalidInput,
            "configure_huggingface",
            "expected an HTTP(S) Hub endpoint without credentials, query or fragment",
        )
    };
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
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "open_huggingface_dataset",
            "partial exports require explicit opt-in",
        ));
    }
    Ok(())
}

pub(super) fn validate_commit_sha(sha: &str) -> Result<()> {
    if !is_commit_sha(sha) {
        return Err(Error::new(
            ErrorKind::Decode,
            "resolve_huggingface",
            "revision metadata did not contain a commit SHA",
        ));
    }
    Ok(())
}

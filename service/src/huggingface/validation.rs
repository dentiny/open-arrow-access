use open_arrow_access_core::{Error, ErrorKind, Result};

use super::HuggingFaceRepository;

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

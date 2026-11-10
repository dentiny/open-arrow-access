use open_arrow_access_core::{Error, ErrorKind, Result};

use super::HuggingFaceRepository;

pub(super) fn validate_repository(repository: &HuggingFaceRepository) -> Result<()> {
    validate_repository_id(&repository.repository)?;
    validate_revision(&repository.revision)?;
    validate_path(&repository.root, true)
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

pub(super) fn validate_path(path: &str, directory: bool) -> Result<()> {
    let components = if directory {
        path.strip_suffix('/').unwrap_or(path)
    } else {
        path
    };
    let operation = if directory { "list" } else { "open" };
    let invalid_path = |message| Error::new(ErrorKind::InvalidInput, operation, message);

    if components.is_empty() && !(directory && path.is_empty()) {
        return Err(invalid_path("path must not be empty"));
    }

    if path.trim() != path {
        return Err(invalid_path("path must not have surrounding whitespace"));
    }

    if !components.is_empty()
        && components
            .split('/')
            .any(|part| matches!(part, "" | "." | ".."))
    {
        return Err(invalid_path(
            "path must not contain empty or dot components",
        ));
    }

    if path.contains(['\\', '\0']) {
        return Err(invalid_path("path must not contain backslashes or NUL"));
    }

    Ok(())
}

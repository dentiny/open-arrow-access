use open_arrow_access_core::{Error, ErrorKind, Result};

use super::HuggingFaceRepository;

pub(super) fn validate_repository(repository: &HuggingFaceRepository) -> Result<()> {
    let parts: Vec<_> = repository.repository.split('/').collect();
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
    if repository.revision.is_empty()
        || repository.revision.trim() != repository.revision
        || repository.revision.contains(['\0', '\\'])
        || matches!(repository.revision.as_str(), "." | "..")
    {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "configure_huggingface",
            "invalid repository revision",
        ));
    }
    validate_path(&repository.root, true)
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

    if components
        .split('/')
        .any(|part| part == "." || part == ".." || (!components.is_empty() && part.is_empty()))
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

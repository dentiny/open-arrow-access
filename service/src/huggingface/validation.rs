use open_arrow_access_core::{Error, ErrorKind, Result};

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

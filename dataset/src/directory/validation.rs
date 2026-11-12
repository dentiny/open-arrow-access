use glob::Pattern;
use open_arrow_access_core::{Error, ErrorKind, Result};

pub(super) fn resolve_glob(path: &str) -> Result<(&str, Option<Pattern>)> {
    let Some(wildcard) = path.find(['*', '?', '[']) else {
        return Ok((path, None));
    };
    let split = path[..wildcard].rfind('/').map_or(0, |slash| slash + 1);
    let (prefix, pattern) = path.split_at(split);
    let pattern = Pattern::new(pattern).map_err(|error| {
        Error::new(
            ErrorKind::InvalidInput,
            "select_directory",
            error.to_string(),
        )
        .with_source(error)
    })?;
    Ok((prefix, Some(pattern)))
}

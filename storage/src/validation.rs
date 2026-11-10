use open_arrow_access_core::{Error, ErrorKind, Result};

pub(crate) fn validate_locator(path: &str, prefix: bool, operation: &'static str) -> Result<()> {
    let base = if prefix {
        path.strip_suffix('/').unwrap_or(path)
    } else {
        path
    };
    if path.trim() != path
        || path.starts_with('/')
        || (!prefix && path.is_empty())
        || (!base.is_empty() && base.split('/').any(|part| matches!(part, "" | "." | "..")))
    {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            operation,
            "expected a canonical root-relative locator",
        ));
    }
    Ok(())
}

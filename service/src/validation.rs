/// Validate a nonempty relative file path without normalizing its components.
/// Non-control whitespace within components and URL-reserved characters are preserved.
pub(crate) fn valid_relative_file_path(path: &str) -> bool {
    open_arrow_access_core::is_relative_path(path)
        && path.trim() == path
        && !path.contains('\\')
        && !path.chars().any(char::is_control)
}

#[cfg(test)]
#[path = "../tests/unit/path_validation.rs"]
mod tests;

/// Validate a nonempty relative file path without normalizing its components.
/// Non-control whitespace within components and URL-reserved characters are preserved.
pub(crate) fn valid_relative_file_path(path: &str) -> bool {
    path.split('/').all(|part| !matches!(part, "" | "." | ".."))
        && path.trim() == path
        && !path.contains('\\')
        && !path.chars().any(char::is_control)
}

#[cfg(test)]
#[path = "../tests/unit/path_validation.rs"]
mod tests;

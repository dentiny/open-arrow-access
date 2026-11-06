/// Check a slash-separated locator without normalizing away dot components.
/// Empty paths are allowed so a directory request can select the accessor root.
pub(super) fn is_root_relative(path: &str) -> bool {
    !path.starts_with('/') && !path.split('/').any(|part| matches!(part, "." | ".."))
}

/// Normalize directory separators after rejecting rooted and dot-component paths.
pub(super) fn normalize_prefix(path: &str) -> Option<String> {
    if !is_root_relative(path) {
        return None;
    }
    let mut prefix = String::with_capacity(path.len());
    for part in path.split('/').filter(|part| !part.is_empty()) {
        if !prefix.is_empty() {
            prefix.push('/');
        }
        prefix.push_str(part);
    }
    Some(prefix)
}

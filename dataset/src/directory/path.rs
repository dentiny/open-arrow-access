/// Check a slash-separated locator without normalizing away dot components.
/// Empty paths are allowed so a directory request can select the accessor root.
pub(super) fn is_root_relative(path: &str) -> bool {
    !path.starts_with('/') && !path.split('/').any(|part| matches!(part, "." | ".."))
}

use super::error::invalid_line;
use open_arrow_access_core::{ObjectRef, Result};
use std::collections::HashSet;

pub(super) fn validate_selection(selected: &[ObjectRef], prefix: &str) -> Result<()> {
    if selected.is_empty() {
        return Err(super::error::empty_manifest_selection(prefix));
    }
    Ok(())
}

pub(super) fn validate_unique_path(
    seen: &mut HashSet<String>,
    path: &str,
    number: usize,
) -> Result<()> {
    if !seen.insert(path.to_owned()) {
        return Err(invalid_line(number, "duplicate selected file"));
    }
    Ok(())
}

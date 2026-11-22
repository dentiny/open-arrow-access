use super::super::{error, pages::identity::number};
use open_arrow_access_core::Result;

pub(super) fn group(prefix: &str) -> Result<&str> {
    let invalid = || {
        error::invalid_pages(format!(
            "expected ocr/item-<positive-id>/ or ocr/part-<positive-id>/, got {prefix:?}"
        ))
    };
    let group = prefix
        .strip_prefix("ocr/")
        .and_then(|value| value.strip_suffix('/'))
        .ok_or_else(invalid)?;
    let id = group
        .strip_prefix("item-")
        .or_else(|| group.strip_prefix("part-"))
        .ok_or_else(invalid)?;
    number(id).map_err(|_| invalid())?;
    Ok(group)
}

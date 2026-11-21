use super::error;
use open_arrow_access_core::{ObjectRef, Result};

pub(super) fn validate_objectds(objects: &[ObjectRef], allow_empty: bool) -> Result<()> {
    if !allow_empty && objects.is_empty() {
        return Err(error::empty_open_data_partition());
    }
    for object in objects {
        validate_open_data_key(object.locator(), false)?;
    }
    Ok(())
}

pub(super) fn validate_open_data_prefix(prefix: &str) -> Result<()> {
    validate_open_data_key(prefix.trim_end_matches('/'), true)
}

pub(super) fn select_open_data_file(relative: &str) -> Result<bool> {
    Ok(!relative.is_empty() && !relative.ends_with('/'))
}

fn validate_open_data_key(key: &str, prefix: bool) -> Result<()> {
    let mut parts = key.split('/');
    if !matches!(parts.next(), Some("ocr" | "images" | "scandata" | "data"))
        || key.contains(['\\', '?', '#'])
        || key.chars().any(char::is_control)
    {
        return Err(error::invalid_open_data_key());
    }
    let mut has_child = false;
    for part in parts {
        if matches!(part, "" | "." | "..") {
            return Err(error::invalid_open_data_component());
        }
        has_child = true;
    }
    if !prefix && !has_child {
        return Err(error::open_data_object_required());
    }
    Ok(())
}

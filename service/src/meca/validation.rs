use super::error;
use open_arrow_access_core::Result;

pub(super) fn validate_item_id(id: &str) -> Result<()> {
    if id.is_empty() {
        return Err(error::decode("MECA item id is empty"));
    }
    Ok(())
}

pub(super) fn validate_href(href: &str, is_absolute: bool) -> Result<()> {
    if is_absolute || href.contains(['?', '#']) {
        return Err(error::decode("MECA href must be a relative member URI"));
    }
    Ok(())
}

pub(super) fn validate_path(path: &str) -> Result<()> {
    if !crate::validation::valid_relative_file_path(path) {
        return Err(error::decode("unsafe MECA member path"));
    }
    Ok(())
}

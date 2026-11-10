use super::error;
use open_arrow_access_core::Result;

pub(super) fn validate_item_id(id: &str) -> Result<()> {
    if id.is_empty() {
        return Err(error::decode("MECA item id is empty"));
    }
    Ok(())
}

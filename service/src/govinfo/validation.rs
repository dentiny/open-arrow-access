use super::error::{decode, invalid};
use crate::common::{array, text};
use open_arrow_access_core::Result;
use serde_json::Value;

pub(super) fn validate_collections(value: &Value) -> Result<()> {
    array(value, "collections")?;
    Ok(())
}

pub(super) fn validate_packages(value: &Value) -> Result<()> {
    array(value, "packages")?;
    Ok(())
}

pub(super) fn validate_page_size(value: u16) -> Result<()> {
    if value == 0 || value > 1000 {
        return Err(invalid("GovInfo pageSize must be 1..=1000"));
    }
    Ok(())
}

pub(super) fn validate_package(value: &Value, id: &str) -> Result<()> {
    if text(value, "packageId")? != id {
        return Err(decode("GovInfo package identity mismatch"));
    }
    Ok(())
}

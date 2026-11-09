use super::error::{decode, invalid};
use crate::common::{array, text};
use open_arrow_access_core::Result;
use serde_json::Value;

pub(super) fn validate_key(value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(invalid("Semantic Scholar requires an API key"));
    }
    Ok(())
}

pub(super) fn validate_release(value: &Value, id: &str) -> Result<()> {
    let resolved = text(value, "release_id")?;
    if resolved == "latest" || (id != "latest" && resolved != id) {
        return Err(decode("release identity mismatch"));
    }
    array(value, "datasets")?;
    Ok(())
}

pub(super) fn validate_diffs(value: &Value) -> Result<()> {
    array(value, "diffs")?;
    Ok(())
}

pub(super) fn validate_dataset(value: &Value, name: &str) -> Result<()> {
    if text(value, "name")? != name {
        return Err(decode("dataset identity mismatch"));
    }
    Ok(())
}

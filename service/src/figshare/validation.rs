use super::error::{self, decode, invalid};
use open_arrow_access_core::Result;
use serde_json::Value;

pub(super) fn validate_pagination(page: u32, size: u16) -> Result<()> {
    if page == 0 || size == 0 || size > 1000 {
        return Err(invalid(
            "Figshare page must be nonzero; page_size must be 1..=1000",
        ));
    }
    Ok(())
}

pub(super) fn validate_collection(value: &Value) -> Result<()> {
    if !value.is_array() {
        return Err(decode("expected Figshare article array"));
    }
    Ok(())
}

pub(super) fn validate_article(value: &Value, id: u64, version: Option<u32>) -> Result<()> {
    if value.get("id").and_then(Value::as_u64) != Some(id) {
        return Err(decode("Figshare article identity mismatch"));
    }
    if version.is_some_and(|v| value.get("version").and_then(Value::as_u64) != Some(v.into())) {
        return Err(decode("Figshare version identity mismatch"));
    }
    if value.get("is_embargoed").and_then(Value::as_bool) == Some(true) {
        return Err(error::embargo());
    }
    Ok(())
}

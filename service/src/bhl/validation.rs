use super::error::{self, decode, invalid};
use crate::common::{array, text};
use open_arrow_access_core::Result;
use serde_json::Value;

pub(super) fn validate_key(value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(invalid("empty BHL API key"));
    }
    Ok(())
}

pub(super) fn validate_parameters(parameters: &[(&str, &str)]) -> Result<()> {
    crate::common::reject_reserved_parameters(
        parameters,
        &["op", "format", "apikey", "callback"],
        "BHL control/auth parameters are configured by the service",
    )
}

pub(super) fn validate_response(value: &Value) -> Result<()> {
    match text(value, "Status")? {
        "ok" => {}
        "unauthorized" => return Err(error::unauthorized()),
        _ => return Err(decode("BHL returned an API error")),
    }
    array(value, "Result")?;
    Ok(())
}

pub(super) fn validate_item(items: &[Value], id: u64) -> Result<()> {
    if items.len() != 1 || items[0].get("ItemID").and_then(Value::as_u64) != Some(id) {
        return Err(decode("BHL item identity mismatch"));
    }
    Ok(())
}

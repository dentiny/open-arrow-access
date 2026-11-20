use super::error;
use crate::common::{array, text};
use open_arrow_access_core::{ObjectRef, Result};
use serde_json::Value;

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
        _ => return Err(error::api_error()),
    }
    array(value, "Result")?;
    Ok(())
}

pub(super) fn validate_item(items: &[Value], id: u64) -> Result<()> {
    if items.len() != 1 || items[0].get("ItemID").and_then(Value::as_u64) != Some(id) {
        return Err(error::item_identity_mismatch());
    }
    Ok(())
}

pub(super) fn validate_open_data_partition(objects: &[ObjectRef]) -> Result<()> {
    if objects.is_empty() {
        return Err(error::empty_open_data_partition());
    }
    validate_open_data_objects(objects)
}

pub(super) fn validate_open_data_objects(objects: &[ObjectRef]) -> Result<()> {
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

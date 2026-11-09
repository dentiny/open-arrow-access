//! JSON parsing and borrowed field access shared by providers.
use super::error;
use open_arrow_access_core::Result;
use serde_json::Value;

/// Read a required nonempty JSON string field.
pub fn text<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| error::missing_field(field))
}

/// Read a required JSON array; missing and null fields are errors.
pub fn array<'a>(value: &'a Value, field: &str) -> Result<&'a Vec<Value>> {
    value
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| error::missing_field(field))
}

pub(super) fn parse(bytes: &[u8]) -> Result<Value> {
    serde_json::from_slice(bytes).map_err(error::json)
}

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

/// Map a JSON URL field to a logical file name.
#[derive(Clone, Copy, Debug)]
pub struct LinkField<'a> {
    /// JSON metadata key containing the URL.
    pub json_field: &'a str,
    /// Logical file name exposed by the provider.
    pub file_name: &'a str,
}

/// An available metadata representation and its download URL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataLink {
    pub file_name: String,
    pub url: String,
}

/// Resolve optional HTTP representations in caller-selected field order.
/// Missing/null/empty links mean that representation is unavailable; wrong types fail.
/// Logical-name uniqueness remains the responsibility of the shared file accessor.
pub fn optional_links(value: &Value, fields: &[LinkField<'_>]) -> Result<Vec<MetadataLink>> {
    if !value.is_object() {
        return Err(error::decode("expected metadata object"));
    }
    let mut files = Vec::new();
    for field in fields {
        match value.get(field.json_field) {
            Some(Value::String(url)) if !url.is_empty() => {
                super::resolve_download_url(url)?;
                files.push(MetadataLink {
                    file_name: field.file_name.into(),
                    url: url.clone(),
                });
            }
            None | Some(Value::Null) | Some(Value::String(_)) => {}
            _ => return Err(error::missing_field(field.json_field)),
        }
    }
    Ok(files)
}

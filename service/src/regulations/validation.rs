use super::RegulationsResource;
use crate::common::{array, decode, invalid, text};
use open_arrow_access_core::Result;
use serde_json::Value;

pub(super) fn validate_pagination(parameters: &[(&str, &str)], page: u8, size: u16) -> Result<()> {
    if !(1..=20).contains(&page) || !(5..=250).contains(&size) {
        return Err(invalid(
            "Regulations.gov page must be 1..=20 and size 5..=250",
        ));
    }
    crate::common::reject_reserved_parameters(
        parameters,
        &["page[number]", "page[size]"],
        "pagination is configured separately from filters",
    )
}

pub(super) fn validate_search_response(value: &Value) -> Result<()> {
    array(value, "data")?;
    let meta = value
        .get("meta")
        .and_then(Value::as_object)
        .ok_or_else(|| decode("missing Regulations.gov pagination metadata"))?;
    if !meta.contains_key("hasNextPage") {
        return Err(decode("missing hasNextPage"));
    }
    Ok(())
}

pub(super) fn validate_record(
    record: &Value,
    resource: RegulationsResource,
    id: &str,
) -> Result<()> {
    if text(record, "id")? != id || text(record, "type")? != resource.name() {
        return Err(decode("Regulations.gov resource identity mismatch"));
    }
    Ok(())
}

pub(super) fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(invalid("Regulations.gov requires an API key"));
    }
    Ok(())
}

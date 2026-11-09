use super::error::{decode, invalid};
use crate::common::array;
use open_arrow_access_core::Result;
use serde_json::Value;
use url::Url;

pub(super) fn validate_search_request(
    parameters: &[(&str, &str)],
    page: u32,
    count: u16,
) -> Result<()> {
    if page == 0 || count == 0 {
        return Err(invalid("LoC page and count must be nonzero"));
    }
    if parameters
        .iter()
        .any(|(name, _)| matches!(*name, "fo" | "sp" | "c" | "at" | "callback"))
    {
        return Err(invalid(
            "LoC response format and pagination are configured by the service",
        ));
    }
    Ok(())
}

pub(super) fn validate_search_response(value: &Value) -> Result<()> {
    array(value, "results")?;
    Ok(())
}

pub(super) fn validate_item_response(value: &Value) -> Result<()> {
    if !value.get("item").is_some_and(Value::is_object) {
        return Err(decode("missing LoC item metadata"));
    }
    Ok(())
}

pub(super) fn validate_ocr_request(segment: &str, format: &str) -> Result<()> {
    if segment.is_empty() || format.is_empty() {
        return Err(invalid("OCR segment and format must be nonempty"));
    }
    Ok(())
}

pub(super) fn validate_ocr_response(value: &Value, segment: &str) -> Result<()> {
    if !value
        .get(segment)
        .and_then(|record| record.get("full_text"))
        .is_some_and(Value::is_string)
    {
        return Err(decode("missing LoC OCR full_text response"));
    }
    Ok(())
}

pub(super) fn validate_metadata_endpoint(url: &Url, prefix: &str) -> Result<()> {
    if !url.path().starts_with(prefix) {
        return Err(invalid("unexpected LoC metadata endpoint"));
    }
    Ok(())
}

pub(super) fn validate_representation(value: &Value) -> Result<()> {
    if !value.is_object() {
        return Err(decode("invalid LoC representation"));
    }
    Ok(())
}

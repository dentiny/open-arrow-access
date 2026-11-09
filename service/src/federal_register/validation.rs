use super::error::decode;
use crate::common::{array, text};
use open_arrow_access_core::Result;
use serde_json::Value;

pub(super) fn validate_search(value: &Value) -> Result<()> {
    array(value, "results")?;
    Ok(())
}

pub(super) fn validate_document(value: &Value, number: &str) -> Result<()> {
    if text(value, "document_number")? != number {
        return Err(decode("Federal Register document identity mismatch"));
    }
    Ok(())
}

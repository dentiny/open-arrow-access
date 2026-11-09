use super::error::{decode, invalid};
use crate::common::{array, text};
use open_arrow_access_core::Result;
use serde_json::Value;

pub(super) fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(invalid("USPTO requires an ODP API key"));
    }
    Ok(())
}

pub(super) fn validate_limit(limit: u16) -> Result<()> {
    if limit == 0 {
        return Err(invalid("USPTO file page limit must be nonzero"));
    }
    Ok(())
}

pub(super) fn validate_products(value: &Value) -> Result<()> {
    array(value, "bulkDataProductBag")?;
    Ok(())
}

pub(super) fn validate_product(products: &[Value], id: &str) -> Result<()> {
    if products.len() != 1 || !text(&products[0], "productIdentifier")?.eq_ignore_ascii_case(id) {
        return Err(decode("USPTO product identity mismatch"));
    }
    Ok(())
}

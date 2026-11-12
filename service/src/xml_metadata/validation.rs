use super::{error, parser::XmlDocument};
use open_arrow_access_core::Result;

pub(super) fn validate_root_name(name: &str, expected: &str) -> Result<()> {
    if name != expected {
        return Err(error::root(expected));
    }
    Ok(())
}

pub(super) fn validate_doctype(value: &str, duplicate: bool) -> Result<()> {
    if duplicate || value.contains('[') {
        return Err(error::invalid(
            "duplicate DTDs and internal subsets are unsupported",
        ));
    }
    Ok(())
}

/// Check the document root without fetching a DTD or decoding record values.
pub fn validate_root(bytes: &[u8], expected: &str) -> Result<()> {
    XmlDocument::new(bytes, expected).map(|_| ())
}

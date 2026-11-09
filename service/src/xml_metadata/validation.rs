use super::error;
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

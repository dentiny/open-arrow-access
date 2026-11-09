use super::error;
use open_arrow_access_core::Result;

pub(super) fn validate_root_name(name: &[u8], expected: &str) -> Result<()> {
    if name != expected.as_bytes() {
        return Err(error::root(expected));
    }
    Ok(())
}

pub(super) fn validate_doctype(value: &[u8], duplicate: bool) -> Result<()> {
    if duplicate || value.contains(&b'[') {
        return Err(error::invalid(
            "duplicate DTDs and internal subsets are unsupported",
        ));
    }
    Ok(())
}

use super::error::{decode, invalid};
use open_arrow_access_core::Result;
use serde_json::Value;

pub(super) fn validate_repository(value: &str) -> Result<()> {
    if !value.starts_with("osbooks-") {
        return Err(invalid("expected an OpenStax osbooks-* repository"));
    }
    Ok(())
}

pub(super) fn validate_commit_sha(sha: &str) -> Result<()> {
    if sha.len() != 40 || !sha.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(decode("invalid GitHub commit SHA"));
    }
    Ok(())
}

pub(super) fn validate_pagination(page: u32, size: u16) -> Result<()> {
    if page == 0 || size == 0 || size > 100 {
        return Err(invalid("GitHub page must be nonzero and per_page 1..=100"));
    }
    Ok(())
}

pub(super) fn validate_repositories(value: &Value) -> Result<()> {
    if !value.is_array() {
        return Err(decode("expected GitHub repository array"));
    }
    Ok(())
}

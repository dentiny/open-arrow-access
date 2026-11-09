use super::validation;
use crate::common::text;
use open_arrow_access_core::Result;
use serde_json::Value;

pub(super) fn parse_commit_sha(value: &Value) -> Result<&str> {
    let sha = text(value, "sha")?;
    validation::validate_commit_sha(sha)?;
    Ok(sha)
}

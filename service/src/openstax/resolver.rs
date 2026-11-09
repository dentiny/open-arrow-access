use super::validation;
use crate::common::filename;
use open_arrow_access_core::Result;

pub(super) fn resolve_repository(value: &str) -> Result<String> {
    validation::validate_repository(value)?;
    filename(value)
}

use super::validation;
use crate::common::{MetadataLink, array, resolve_named_links};
use open_arrow_access_core::Result;
use serde_json::Value;

pub(super) fn parse_article_files(
    value: &Value,
    id: u64,
    version: Option<u32>,
) -> Result<Vec<MetadataLink>> {
    validation::validate_article(value, id, version)?;
    resolve_named_links(array(value, "files")?, "name", "download_url")
}

use super::validation;
use crate::common::{MetadataLink, array, text, validate_filename};
use open_arrow_access_core::Result;
use serde_json::Value;

pub(super) fn parse_article_files(
    value: &Value,
    id: u64,
    version: Option<u32>,
) -> Result<Vec<MetadataLink>> {
    validation::validate_article(value, id, version)?;
    array(value, "files")?
        .iter()
        .map(|file| {
            let name = text(file, "name")?;
            validate_filename(name)?;
            Ok(MetadataLink {
                file_name: name.into(),
                url: text(file, "download_url")?.into(),
            })
        })
        .collect()
}

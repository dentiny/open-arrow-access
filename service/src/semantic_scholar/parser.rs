use super::{error::decode, validation};
use crate::common::{MetadataLink, array};
use open_arrow_access_core::Result;
use serde_json::Value;

pub(super) fn parse_dataset_files(value: &Value, name: &str) -> Result<Vec<MetadataLink>> {
    validation::validate_dataset(value, name)?;
    array(value, "files")?
        .iter()
        .enumerate()
        .map(|(i, url)| {
            Ok(MetadataLink {
                file_name: format!("part-{i:05}"),
                url: url
                    .as_str()
                    .ok_or_else(|| decode("invalid partition URL"))?
                    .to_owned(),
            })
        })
        .collect()
}

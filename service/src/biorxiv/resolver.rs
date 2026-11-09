use crate::common::{LinkField, MetadataLink, array, resolve_optional_links};
use open_arrow_access_core::Result;
use serde_json::Value;

pub(super) fn resolve_jats_files(value: &Value) -> Result<Vec<MetadataLink>> {
    let mut files = Vec::new();
    for (i, paper) in array(value, "collection")?.iter().enumerate() {
        for link in resolve_optional_links(
            paper,
            &[LinkField {
                json_field: "jatsxml",
                file_name: "xml",
            }],
        )? {
            files.push(MetadataLink {
                file_name: format!("paper-{i:05}.xml"),
                url: link.url,
            });
        }
    }
    Ok(files)
}

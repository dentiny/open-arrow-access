use crate::common::{MetadataLink, array};
use crate::discovery::links::resolve_optional_fields;
use open_arrow_access_core::Result;
use serde_json::Value;

pub(super) fn resolve_jats_files(value: &Value) -> Result<Vec<MetadataLink>> {
    let mut files = Vec::new();
    for (i, paper) in array(value, "collection")?.iter().enumerate() {
        files.extend(resolve_optional_fields(
            paper,
            [("jatsxml", "xml")],
            |_, url| MetadataLink {
                file_name: format!("paper-{i:05}.xml"),
                url: url.into(),
            },
        )?);
    }
    Ok(files)
}

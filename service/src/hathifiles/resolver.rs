use super::error::decode;
use crate::common::{Connection, MetadataLink, filename, text};
use open_arrow_access_core::Result;
use serde_json::Value;

pub(super) fn resolve_catalog_files(
    connection: &Connection,
    value: &Value,
) -> Result<Vec<MetadataLink>> {
    let entries = value
        .as_array()
        .ok_or_else(|| decode("expected Hathifiles listing array"))?;
    entries
        .iter()
        .map(|entry| {
            let name = text(entry, "filename")?;
            let path = filename(name)?;
            Ok(MetadataLink {
                file_name: name.into(),
                url: connection
                    .url(&format!("files/hathifiles/{path}"), &[])?
                    .into(),
            })
        })
        .collect()
}

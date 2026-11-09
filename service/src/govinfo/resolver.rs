use super::{error::decode, validation};
use crate::common::{Connection, MetadataLink, array, encode_path, filename, text};
use open_arrow_access_core::Result;
use serde_json::Value;

pub(super) fn resolve_prefix(value: &str) -> Result<String> {
    if value.is_empty() {
        return Ok(String::new());
    }
    encode_path(value)
}

pub(super) fn resolve_bulk_files(
    connection: &Connection,
    prefix: &str,
    value: &Value,
) -> Result<Vec<MetadataLink>> {
    let mut files = Vec::new();
    for entry in array(value, "files")? {
        let name = text(entry, "name")?;
        let path = filename(name)?;
        match entry.get("folder").and_then(Value::as_bool) {
            Some(false) => files.push(MetadataLink {
                file_name: name.into(),
                url: connection
                    .url(&format!("bulkdata/{prefix}/{path}"), &[])?
                    .into(),
            }),
            Some(true) => {}
            None => return Err(decode("bulk entry lacks folder flag")),
        }
    }
    Ok(files)
}

pub(super) fn resolve_package_files(value: &Value, id: &str) -> Result<Vec<MetadataLink>> {
    validation::validate_package(value, id)?;
    let downloads = value
        .get("download")
        .and_then(Value::as_object)
        .ok_or_else(|| decode("missing package download map"))?;
    downloads
        .iter()
        .map(|(name, url)| {
            Ok(MetadataLink {
                file_name: name.clone(),
                url: url
                    .as_str()
                    .ok_or_else(|| decode("invalid package download URL"))?
                    .to_owned(),
            })
        })
        .collect()
}

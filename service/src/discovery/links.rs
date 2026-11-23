//! Provider-independent link descriptions and pure metadata extraction.
use crate::common::{error, text};
use open_arrow_access_core::Result;
use serde_json::Value;

/// Map a JSON URL field to a logical file name.
#[derive(Clone, Copy, Debug)]
pub struct LinkField<'a> {
    pub json_field: &'a str,
    pub file_name: &'a str,
}

/// An available metadata representation and its download URL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataLink {
    pub file_name: String,
    pub url: String,
}

/// Resolve a list of files with required name and download URL string fields.
/// Order and URL strings are preserved; URL and namespace validation happen
/// when binding downloads.
pub fn resolve_named_links(
    files: &[Value],
    name_field: &str,
    url_field: &str,
) -> Result<Vec<MetadataLink>> {
    files
        .iter()
        .map(|file| {
            let name = text(file, name_field)?;
            Ok(MetadataLink {
                file_name: name.into(),
                url: text(file, url_field)?.into(),
            })
        })
        .collect()
}

/// Resolve optional HTTP links in selector order, omitting missing/null/empty links.
/// Invalid types/URLs fail; namespace binding checks logical-name uniqueness.
pub fn resolve_optional_links(
    value: &Value,
    fields: &[LinkField<'_>],
) -> Result<Vec<MetadataLink>> {
    if !value.is_object() {
        return Err(error::decode("expected metadata object"));
    }
    let mut files = Vec::new();
    for field in fields {
        match value.get(field.json_field) {
            Some(Value::String(url)) if !url.is_empty() => {
                crate::common::resolve_download_url(url)?;
                files.push(MetadataLink {
                    file_name: field.file_name.into(),
                    url: url.clone(),
                });
            }
            None | Some(Value::Null) | Some(Value::String(_)) => {}
            _ => return Err(error::missing_field(field.json_field)),
        }
    }
    Ok(files)
}

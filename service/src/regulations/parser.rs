use super::{RegulationsResource, error::decode, validation};
use crate::common::{MetadataLink, text};
use open_arrow_access_core::Result;
use serde_json::Value;

fn parse_representations(
    record: &Value,
    prefix: &str,
    files: &mut Vec<MetadataLink>,
) -> Result<()> {
    let attributes = record
        .get("attributes")
        .and_then(Value::as_object)
        .ok_or_else(|| decode("missing resource attributes"))?;
    if let Some(formats) = attributes.get("fileFormats") {
        let formats = formats
            .as_array()
            .ok_or_else(|| decode("invalid fileFormats"))?;
        for (i, format) in formats.iter().enumerate() {
            files.push(MetadataLink {
                file_name: format!("{prefix}/file-{i:03}"),
                url: text(format, "fileUrl")?.into(),
            });
        }
    }
    Ok(())
}

pub(super) fn parse_record_files(
    value: &Value,
    resource: RegulationsResource,
    id: &str,
) -> Result<Vec<MetadataLink>> {
    let record = value
        .get("data")
        .ok_or_else(|| decode("missing JSON:API data resource"))?;
    validation::validate_record(record, resource, id)?;
    let mut files = Vec::new();
    parse_representations(record, "document", &mut files)?;
    if let Some(included) = value.get("included") {
        for (i, attachment) in included
            .as_array()
            .ok_or_else(|| decode("invalid included resources"))?
            .iter()
            .enumerate()
        {
            if text(attachment, "type")? == "attachments" {
                parse_representations(attachment, &format!("attachment-{i:03}"), &mut files)?;
            }
        }
    }
    Ok(files)
}

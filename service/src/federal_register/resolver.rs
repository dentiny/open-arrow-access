use super::validation;
use crate::common::MetadataLink;
use crate::discovery::links::resolve_optional_fields;
use open_arrow_access_core::Result;
use serde_json::Value;

pub(super) fn resolve_document_files(value: &Value, number: &str) -> Result<Vec<MetadataLink>> {
    validation::validate_document(value, number)?;
    resolve_optional_fields(
        value,
        [
            "pdf_url",
            "full_text_xml_url",
            "raw_text_url",
            "body_html_url",
            "html_url",
            "public_inspection_pdf_url",
        ]
        .into_iter()
        .map(|name| (name, name)),
        |name, url| MetadataLink {
            file_name: name.into(),
            url: url.into(),
        },
    )
}

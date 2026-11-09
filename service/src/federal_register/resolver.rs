use super::validation;
use crate::common::{LinkField, MetadataLink, resolve_optional_links};
use open_arrow_access_core::Result;
use serde_json::Value;

pub(super) fn resolve_document_files(value: &Value, number: &str) -> Result<Vec<MetadataLink>> {
    validation::validate_document(value, number)?;
    resolve_optional_links(
        value,
        &[
            LinkField {
                json_field: "pdf_url",
                file_name: "pdf_url",
            },
            LinkField {
                json_field: "full_text_xml_url",
                file_name: "full_text_xml_url",
            },
            LinkField {
                json_field: "raw_text_url",
                file_name: "raw_text_url",
            },
            LinkField {
                json_field: "body_html_url",
                file_name: "body_html_url",
            },
            LinkField {
                json_field: "html_url",
                file_name: "html_url",
            },
            LinkField {
                json_field: "public_inspection_pdf_url",
                file_name: "public_inspection_pdf_url",
            },
        ],
    )
}

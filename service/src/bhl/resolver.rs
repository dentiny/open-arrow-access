use super::validation;
use crate::common::{LinkField, MetadataLink, array, resolve_optional_links};
use open_arrow_access_core::Result;
use serde_json::Value;

pub(super) fn resolve_item_files(value: &Value, id: u64) -> Result<Vec<MetadataLink>> {
    let items = array(value, "Result")?;
    validation::validate_item(items, id)?;
    resolve_optional_links(
        &items[0],
        &[
            LinkField {
                json_field: "ItemTextUrl",
                file_name: "text",
            },
            LinkField {
                json_field: "ItemPDFUrl",
                file_name: "pdf",
            },
            LinkField {
                json_field: "ItemImagesUrl",
                file_name: "images",
            },
        ],
    )
}

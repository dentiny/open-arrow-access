use super::{error::decode, validation};
use crate::common::{MetadataLink, array, resolve_named_links};
use open_arrow_access_core::Result;
use serde_json::Value;

pub(super) fn parse_product_files(value: &Value, id: &str) -> Result<Vec<MetadataLink>> {
    let products = array(value, "bulkDataProductBag")?;
    validation::validate_product(products, id)?;
    let bag = products[0]
        .get("productFileBag")
        .ok_or_else(|| decode("missing USPTO product file bag"))?;
    resolve_named_links(array(bag, "fileDataBag")?, "fileName", "fileDownloadURI")
}

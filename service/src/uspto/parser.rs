use super::{error::decode, validation};
use crate::common::{MetadataLink, array, text, validate_filename};
use open_arrow_access_core::Result;
use serde_json::Value;

pub(super) fn parse_product_files(value: &Value, id: &str) -> Result<Vec<MetadataLink>> {
    let products = array(value, "bulkDataProductBag")?;
    validation::validate_product(products, id)?;
    let bag = products[0]
        .get("productFileBag")
        .ok_or_else(|| decode("missing USPTO product file bag"))?;
    array(bag, "fileDataBag")?
        .iter()
        .map(|file| {
            let name = text(file, "fileName")?;
            validate_filename(name)?;
            Ok(MetadataLink {
                file_name: name.into(),
                url: text(file, "fileDownloadURI")?.into(),
            })
        })
        .collect()
}

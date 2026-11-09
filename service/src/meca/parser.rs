use super::{error, validation};
use open_arrow_access_core::Result;
use serde::Deserialize;

#[derive(Deserialize)]
pub(super) struct Manifest {
    #[serde(default)]
    pub(super) item: Vec<Item>,
}

#[derive(Deserialize)]
pub(super) struct Item {
    #[serde(rename = "@id")]
    pub(super) id: String,
    #[serde(rename = "@item-type")]
    pub(super) kind: Option<String>,
    #[serde(default)]
    pub(super) instance: Vec<Instance>,
}

#[derive(Deserialize)]
pub(super) struct Instance {
    #[serde(rename = "@href")]
    pub(super) href: String,
    #[serde(rename = "@media-type")]
    pub(super) media_type: Option<String>,
}

pub(super) fn parse_manifest(bytes: &[u8]) -> Result<Manifest> {
    validation::validate_root(bytes)?;
    quick_xml::de::from_reader(bytes).map_err(error::xml)
}

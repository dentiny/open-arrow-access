//! Direct AWS TSV acquisition and populated page metadata joins.
mod error;
mod join;
mod source;
use super::{BhlPageIdentity, page_schema};
use arrow_array::StructArray;
use arrow_schema::{DataType, Fields};
use open_arrow_access_core::{ObjectRef, Result};
use serde::Serialize;
pub use source::{BhlMetadata, BhlMetadataInfo, SourceInfo};
use std::collections::{BTreeMap, HashMap, hash_map::Entry};

struct Table {
    name: String,
    fields: Fields,
    key: usize,
    single: bool,
}
fn tables() -> Vec<Table> {
    let schema = page_schema();
    let DataType::Struct(children) = schema
        .field_with_name("metadata")
        .expect("fixed schema")
        .data_type()
    else {
        unreachable!()
    };
    children
        .iter()
        .map(|child| {
            let (fields, single) = match child.data_type() {
                DataType::Struct(fields) => (fields.clone(), true),
                DataType::List(element) => {
                    let DataType::Struct(fields) = element.data_type() else {
                        unreachable!()
                    };
                    (fields.clone(), false)
                }
                _ => unreachable!(),
            };
            let key = match child.name().as_str() {
                "title" | "creator" | "subject" | "titleidentifier" => "TitleID",
                "item" => "ItemID",
                "page" | "pagename" => "PageID",
                "part" | "partpage" | "partcreator" | "partidentifier" => "PartID",
                "creatoridentifier" => "CreatorID",
                "doi" => "EntityID",
                _ => unreachable!(),
            };
            let key = fields
                .iter()
                .position(|field| field.name() == key)
                .expect("fixed join key");
            Table {
                name: child.name().clone(),
                fields,
                key,
                single,
            }
        })
        .collect()
}

#[derive(Default, Serialize)]
pub struct BhlMetadataReport {
    pub missing: BTreeMap<String, Vec<String>>,
    pub conflicts: BTreeMap<String, Vec<String>>,
}
/// Selected, populated Arrow metadata with provenance and explicit join-gap reports.
pub struct BhlPageMetadata {
    rows: HashMap<BhlPageIdentity, StructArray>,
    pub info: BhlMetadataInfo,
    pub report: BhlMetadataReport,
}
impl BhlMetadata {
    /// Join selected OCR page identities from the already-loaded AWS source rows.
    /// Gaps/conflicts are retained in the report; the outer metadata is populated.
    pub fn select(&self, keys: &[ObjectRef]) -> Result<BhlPageMetadata> {
        let mut rows = HashMap::new();
        let mut report = BhlMetadataReport::default();
        for key in keys {
            let identity = BhlPageIdentity::parse(key.locator())?;
            if let Entry::Vacant(entry) = rows.entry(identity) {
                let (metadata, missing, conflicts) = join::page(self, entry.key())?;
                if !missing.is_empty() {
                    report.missing.insert(key.locator().into(), missing);
                }
                if !conflicts.is_empty() {
                    report.conflicts.insert(key.locator().into(), conflicts);
                }
                entry.insert(metadata);
            }
        }
        Ok(BhlPageMetadata {
            rows,
            info: self.info.clone(),
            report,
        })
    }
}
impl BhlPageMetadata {
    pub fn get(&self, identity: &BhlPageIdentity) -> Result<StructArray> {
        self.rows
            .get(identity)
            .cloned()
            .ok_or_else(|| error::invalid("page is outside metadata selection"))
    }
    pub fn schema_metadata(&self) -> Result<HashMap<String, String>> {
        Ok(HashMap::from([
            (
                "bhl.metadata.fingerprint".into(),
                self.info.fingerprint.clone(),
            ),
            (
                "bhl.metadata.sources".into(),
                serde_json::to_string(&self.info.sources).map_err(error::json)?,
            ),
            (
                "bhl.metadata.reconciliation".into(),
                serde_json::to_string(&self.report).map_err(error::json)?,
            ),
        ]))
    }
}
#[cfg(test)]
mod tests;

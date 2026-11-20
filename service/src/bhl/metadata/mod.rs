//! One immutable, indexed metadata snapshot shared by page conversion jobs.
//! Preparation streams the 13 public gzip TSVs once. Job lookup reads selected
//! IDs only; it never rescans the source corpus or invents a null metadata column.
mod error;
mod index;
mod join;
use super::{BhlPageIdentity, page_schema};
use arrow_array::StructArray;
use arrow_schema::{DataType, Fields};
pub use index::{BhlMetadataInfo, SourceInfo};
use open_arrow_access_core::{ByteAccessor, ObjectRef, Result};
use serde::Serialize;
use std::{
    collections::{BTreeMap, HashMap, hash_map::Entry},
    path::Path,
};

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

/// Prepare one immutable local/NFS snapshot, resuming committed source tables.
/// `storage` is rooted above the AWS `data/` objects (or a matching pinned mirror).
/// The completed output is published exclusively; existing snapshots are never overwritten.
/// Keep this file on shared worker storage and pass its fingerprint with each job.
pub async fn prepare<A: ByteAccessor + 'static>(
    storage: A,
    source_root: &str,
    output: &Path,
) -> Result<BhlMetadataInfo> {
    index::prepare(storage, source_root, output)
        .await
        .map_err(Into::into)
}
/// Inspect the completed read-only snapshot without fetching source tables.
pub fn inspect(path: &Path) -> Result<BhlMetadataInfo> {
    index::Store::open(path)
        .map(|store| store.info)
        .map_err(Into::into)
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
impl BhlPageMetadata {
    /// Indexed local/NFS I/O. Call from a blocking worker, not a Tokio reactor.
    /// Both the preparation format and caller's expected fingerprint are checked.
    pub fn load(path: &Path, fingerprint: &str, keys: &[ObjectRef]) -> Result<Self> {
        let mut store = index::Store::open(path).map_err(open_arrow_access_core::Error::from)?;
        if store.info.fingerprint != fingerprint {
            return Err(error::invalid("metadata snapshot fingerprint mismatch").into());
        }
        let mut rows = HashMap::new();
        let mut report = BhlMetadataReport::default();
        for key in keys {
            let identity = BhlPageIdentity::parse(key.locator())?;
            if let Entry::Vacant(entry) = rows.entry(identity) {
                let (metadata, missing, conflicts) = join::page(&mut store, entry.key())
                    .map_err(open_arrow_access_core::Error::from)?;
                if !missing.is_empty() {
                    report.missing.insert(key.locator().into(), missing);
                }
                if !conflicts.is_empty() {
                    report.conflicts.insert(key.locator().into(), conflicts);
                }
                entry.insert(metadata);
            }
        }
        Ok(Self {
            rows,
            info: store.info,
            report,
        })
    }
    pub fn get(&self, identity: &BhlPageIdentity) -> Result<StructArray> {
        self.rows
            .get(identity)
            .cloned()
            .ok_or_else(|| error::invalid("page is outside metadata selection").into())
    }
    pub fn schema_metadata(&self) -> Result<HashMap<String, String>> {
        Ok(HashMap::from([
            (
                "bhl.metadata.fingerprint".into(),
                self.info.fingerprint.clone(),
            ),
            (
                "bhl.metadata.sources".into(),
                serde_json::to_string(&self.info.sources).map_err(error::Error::from)?,
            ),
            (
                "bhl.metadata.reconciliation".into(),
                serde_json::to_string(&self.report).map_err(error::Error::from)?,
            ),
        ]))
    }
}
#[cfg(test)]
mod tests;

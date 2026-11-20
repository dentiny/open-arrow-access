//! One immutable, indexed metadata snapshot shared by page conversion jobs.
//! Preparation streams the 13 public gzip TSVs once. Job lookup reads selected
//! IDs only; it never rescans the source corpus or invents a null metadata column.
mod error;
mod index;
use super::page_schema;
use arrow_schema::{DataType, Fields};
pub use index::{BhlMetadataIndex, BhlMetadataInfo, BhlMetadataRow, SourceInfo};
use open_arrow_access_core::{ByteAccessor, Result};
use std::path::Path;

struct Table {
    name: String,
    fields: Fields,
    key: usize,
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
            let fields = match child.data_type() {
                DataType::Struct(fields) => fields.clone(),
                DataType::List(element) => {
                    let DataType::Struct(fields) = element.data_type() else {
                        unreachable!()
                    };
                    fields.clone()
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
    BhlMetadataIndex::open(path).map(|store| store.info)
}

#[cfg(test)]
mod index_tests;

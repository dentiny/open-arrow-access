//! Direct AWS TSV acquisition and populated page metadata joins.
mod database;
mod error;
mod join;
mod source;
mod stream;
mod validation;
use super::{BhlPageIdentity, page_schema};
use crate::duckdb::ManagedConnection;
use arrow_array::StructArray;
use arrow_schema::{DataType, Fields};
use open_arrow_access_core::{ObjectRef, Result};
pub use source::{BhlMetadata, BhlMetadataInfo, SourceInfo};
use std::{collections::HashMap, sync::Arc};

#[derive(Clone)]
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
                single: child.name() == "title",
            }
        })
        .collect()
}

/// One joined page, emitted without collecting the full query result in Rust.
pub struct BhlPageMetadata {
    pub identity: BhlPageIdentity,
    pub object: ObjectRef,
    pub metadata: StructArray,
    /// Required join targets not found for this page, e.g. `item:7` or
    /// `title:unresolved parent`. Missing relationships have empty lists; the page
    /// is still emitted. Empty optional relationships are not reported here.
    pub missing: Vec<String>,
    /// Parent ItemID disagreements or page SequenceOrder mismatches with the
    /// OCR filename. Part page sequences are checked against `partpage`.
    /// These messages accompany the emitted page; callers decide how to handle them.
    pub conflicts: Vec<String>,
}
impl BhlMetadata {
    /// Execute one join query and emit each selected page to the callback.
    /// This is synchronous; call from a blocking worker in async applications.
    /// Returning an error from the callback stops emission immediately.
    pub fn for_each(
        &self,
        keys: &[ObjectRef],
        emit: impl FnMut(BhlPageMetadata) -> Result<()>,
    ) -> Result<()> {
        let connection = self.query_connection()?;
        join::for_each(&connection, keys, emit)
    }

    fn query_connection(&self) -> Result<ManagedConnection> {
        self.connection
            .lock()
            .map_err(|_| error::invalid("DuckDB metadata connection lock poisoned"))?
            .try_clone()
    }

    pub(in crate::bhl) fn stream(
        self: Arc<Self>,
        keys: Vec<ObjectRef>,
    ) -> futures::stream::BoxStream<'static, Result<BhlPageMetadata>> {
        stream::pages(self, keys)
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
        ]))
    }
}
#[cfg(test)]
pub(in crate::bhl) mod tests;

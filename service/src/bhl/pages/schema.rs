use arrow_schema::{DataType, Field, Schema};
use open_arrow_access_core::SchemaRef;
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock},
};

#[derive(Deserialize)]
pub(super) struct TableProfile {
    pub name: String,
    pub columns: Vec<String>,
    pub key: String,
    pub single: bool,
}

pub(super) fn profile() -> &'static [TableProfile] {
    static PROFILE: OnceLock<Vec<TableProfile>> = OnceLock::new();
    PROFILE.get_or_init(|| {
        serde_json::from_str(include_str!("profile.json")).expect("static BHL profile")
    })
}

pub(super) fn fields(table: &TableProfile) -> arrow_schema::Fields {
    table
        .columns
        .iter()
        .map(|name| Field::new(name, DataType::LargeUtf8, false))
        .collect()
}

/// All 104 TSV columns remain lexical LargeUtf8 cells, under 13 named children.
pub fn page_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            let metadata = profile()
                .iter()
                .map(|table| {
                    let record = DataType::Struct(fields(table));
                    let data_type = if table.single {
                        record
                    } else {
                        DataType::List(Arc::new(Field::new("item", record, false)))
                    };
                    Field::new(&table.name, data_type, table.single)
                })
                .collect::<Vec<_>>();
            Arc::new(Schema::new(vec![
                Field::new("entity_type", DataType::Utf8, false),
                Field::new("entity_id", DataType::UInt64, false),
                Field::new("page_id", DataType::UInt64, false),
                Field::new("sequence", DataType::UInt64, false),
                Field::new("data", DataType::LargeBinary, false),
                Field::new("source_path", DataType::LargeUtf8, false),
                Field::new("snapshot_id", DataType::Utf8, false),
                Field::new("source_entry_index", DataType::UInt64, true),
                Field::new("metadata", DataType::Struct(metadata.into()), true),
            ]))
        })
        .clone()
}

pub(super) type Row = BTreeMap<String, String>;

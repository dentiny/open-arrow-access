use super::{
    error::Result,
    index::{Row, Store},
    tables,
};
use crate::bhl::{BhlPageIdentity, page_schema};
use arrow_array::StructArray;
use arrow_schema::Schema;
use open_arrow_access_core::RecordBatchBuilder;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

fn id(row: Option<&Row>, field: &str) -> Option<u64> {
    row?.values.get(field)?.parse().ok().filter(|n| *n > 0)
}
fn same(row: &Row, field: &str, value: u64) -> bool {
    id(Some(row), field) == Some(value)
}
fn singleton(
    store: &mut Store,
    table: &str,
    key: Option<u64>,
    missing: &mut Vec<String>,
) -> Result<Vec<Row>> {
    let rows = store.rows(table, key)?;
    if rows.is_empty() {
        missing.push(format!(
            "{table}:{}",
            key.map(|n| n.to_string())
                .unwrap_or_else(|| "unresolved parent".into())
        ));
    }
    Ok(rows)
}
fn ordered_union(
    store: &mut Store,
    table: &str,
    ids: impl IntoIterator<Item = u64>,
) -> Result<Vec<Row>> {
    let mut rows = BTreeMap::new();
    for key in ids.into_iter().collect::<BTreeSet<_>>() {
        for row in store.rows(table, Some(key))? {
            rows.insert(row.ordinal, row);
        }
    }
    Ok(rows.into_values().collect())
}
pub(super) fn page(
    store: &mut Store,
    key: &BhlPageIdentity,
) -> Result<(StructArray, Vec<String>, Vec<String>)> {
    let mut missing = Vec::new();
    let mut conflicts = Vec::new();
    let page = singleton(store, "page", Some(key.page_id), &mut missing)?;
    let is_part = key.entity_type == "part";
    let part = if is_part {
        singleton(store, "part", Some(key.entity_id), &mut missing)?
    } else {
        vec![]
    };
    let partpage = if is_part {
        store
            .rows("partpage", Some(key.entity_id))?
            .into_iter()
            .filter(|row| same(row, "PageID", key.page_id))
            .collect::<Vec<_>>()
    } else {
        vec![]
    };
    if is_part && partpage.is_empty() {
        missing.push(format!("partpage:{}:{}", key.entity_id, key.page_id));
    }
    let parent = if is_part {
        id(part.first(), "ItemID")
            .or_else(|| id(partpage.first(), "ItemID"))
            .or_else(|| id(page.first(), "ItemID"))
    } else {
        Some(key.entity_id)
    };
    for (name, rows) in [("page", &page), ("part", &part), ("partpage", &partpage)] {
        for row in rows {
            if let Some(other) = id(Some(row), "ItemID")
                && Some(other) != parent
            {
                conflicts.push(format!("{name}.ItemID={other}, selected item={parent:?}"));
            }
        }
    }
    let sequence_rows = if is_part { &partpage } else { &page };
    for row in sequence_rows {
        if let Some(sequence) = id(Some(row), "SequenceOrder")
            && sequence != key.sequence
        {
            conflicts.push(format!(
                "SequenceOrder={sequence}, filename sequence={}",
                key.sequence
            ));
        }
    }
    let item = singleton(store, "item", parent, &mut missing)?;
    let title_id = id(item.first(), "TitleID");
    let title = singleton(store, "title", title_id, &mut missing)?;
    let creator = store.rows("creator", title_id)?;
    let partcreator = store.rows("partcreator", is_part.then_some(key.entity_id))?;
    let creatoridentifier = ordered_union(
        store,
        "creatoridentifier",
        creator
            .iter()
            .chain(&partcreator)
            .filter_map(|r| id(Some(r), "CreatorID")),
    )?;
    let mut doi = BTreeMap::new();
    for (kind, value) in [
        ("Title", title_id),
        ("Item", parent),
        ("Page", Some(key.page_id)),
        ("Part", is_part.then_some(key.entity_id)),
    ] {
        for row in store.rows("doi", value)? {
            if row.values["EntityType"].eq_ignore_ascii_case(kind) {
                doi.insert(row.ordinal, row);
            }
        }
    }
    let mut selected = BTreeMap::from([
        ("title", title),
        ("item", item),
        ("page", page),
        ("part", part),
        ("partpage", partpage),
        ("creator", creator),
        ("partcreator", partcreator),
        ("creatoridentifier", creatoridentifier),
        ("subject", store.rows("subject", title_id)?),
        ("titleidentifier", store.rows("titleidentifier", title_id)?),
        (
            "partidentifier",
            store.rows("partidentifier", is_part.then_some(key.entity_id))?,
        ),
        ("pagename", store.rows("pagename", Some(key.page_id))?),
        ("doi", doi.into_values().collect()),
    ]);
    let field = page_schema()
        .field_with_name("metadata")
        .expect("fixed metadata")
        .clone();
    let mut builder = RecordBatchBuilder::new(Arc::new(Schema::new(vec![field])), None)?;
    builder.push(|row| {
        row.append_struct("metadata", |row| {
            for table in tables() {
                let rows = selected
                    .remove(table.name.as_str())
                    .expect("all tables selected");
                if table.single {
                    if let Some(source) = rows.first() {
                        row.append_struct(&table.name, |record| {
                            for field in &table.fields {
                                record
                                    .append(field.name(), source.values[field.name()].as_str())?;
                            }
                            Ok(())
                        })?;
                    }
                } else {
                    row.append_structs(&table.name, rows, |record, source| {
                        for field in &table.fields {
                            record.append(field.name(), source.values[field.name()].as_str())?;
                        }
                        Ok(())
                    })?;
                }
            }
            Ok(())
        })
    })?;
    let batch = builder.finish()?;
    Ok((
        batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("metadata struct")
            .clone(),
        missing,
        conflicts,
    ))
}

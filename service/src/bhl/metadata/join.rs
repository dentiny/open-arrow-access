use super::{BhlPageMetadata, error, validation};
use crate::bhl::{BhlPageIdentity, page_schema};
use arrow_array::{Array, ArrayRef, ListArray, RecordBatch, StringArray, StructArray, UInt64Array};
use open_arrow_access_core::{ObjectRef, Result};
use std::sync::Arc;

const SELECTION_BATCH_SIZE: usize = 8192;
const CANONICAL_ID_COLUMN: usize = 4;
const CANONICAL_MATCHES_COLUMN: usize = 5;

fn strings(column: &dyn Array, row: usize) -> Result<Vec<String>> {
    let list = column
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| error::invalid("expected a reconciliation list"))?;
    let values = list.value(row);
    let values = values
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| error::invalid("expected reconciliation strings"))?;
    values
        .iter()
        .map(|value| {
            value
                .map(str::to_owned)
                .ok_or_else(|| error::invalid("null reconciliation message"))
        })
        .collect()
}

pub(super) fn for_each(
    connection: &::duckdb::Connection,
    keys: &[ObjectRef],
    mut emit: impl FnMut(BhlPageMetadata) -> Result<()>,
) -> Result<()> {
    if keys.is_empty() {
        return Ok(());
    }
    connection
        .execute_batch(
            "CREATE TEMP TABLE bhl_selection (
         request_id UBIGINT, entity_type VARCHAR, entity_id UBIGINT,
         page_id UBIGINT, sequence UBIGINT);",
        )
        .map_err(error::duckdb)?;
    {
        // Resolve the writable temporary catalog explicitly. With an unnamed
        // catalog DuckDB checks the read-only source database instead.
        let mut appender = connection
            .appender_to_catalog_and_db("bhl_selection", "temp", "main")
            .map_err(error::duckdb)?;
        let mut offset = 0u64;
        for objects in keys.chunks(SELECTION_BATCH_SIZE) {
            let identities = objects
                .iter()
                .map(|object| BhlPageIdentity::parse(object.locator()))
                .collect::<Result<Vec<_>>>()?;
            let end = offset + identities.len() as u64;
            let batch = RecordBatch::try_from_iter([
                (
                    "request_id",
                    Arc::new(UInt64Array::from_iter_values(offset..end)) as ArrayRef,
                ),
                (
                    "entity_type",
                    Arc::new(StringArray::from_iter_values(
                        identities.iter().map(|key| key.entity_type.as_str()),
                    )) as ArrayRef,
                ),
                (
                    "entity_id",
                    Arc::new(UInt64Array::from_iter_values(
                        identities.iter().map(|key| key.entity_id),
                    )) as ArrayRef,
                ),
                (
                    "page_id",
                    Arc::new(UInt64Array::from_iter_values(
                        identities.iter().map(|key| key.page_id),
                    )) as ArrayRef,
                ),
                (
                    "sequence",
                    Arc::new(UInt64Array::from_iter_values(
                        identities.iter().map(|key| key.sequence),
                    )) as ArrayRef,
                ),
            ])
            .map_err(error::arrow)?;
            appender.append_record_batch(batch).map_err(error::duckdb)?;
            offset = end;
        }
        appender.flush().map_err(error::duckdb)?;
    }
    let schema = page_schema();
    let field = schema.field_with_name("metadata").expect("fixed metadata");
    let mut statement = connection
        .prepare(include_str!("join.sql"))
        .map_err(error::duckdb)?;
    let _ = statement.stream_arrow([]).map_err(error::duckdb)?;
    let mut joined_rows = 0;
    while let Some(batch) = statement.step().map_err(error::duckdb)? {
        let batch = RecordBatch::from(&batch);
        let indices = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| error::invalid("expected request IDs"))?;
        let matches = batch
            .column(CANONICAL_MATCHES_COLUMN)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| error::invalid("expected canonical match counts"))?;
        let selected = batch
            .column(CANONICAL_ID_COLUMN)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| error::invalid("expected canonical request IDs"))?;
        // Match the fixed nested LargeUtf8/nullability contract.
        let metadata =
            arrow_cast::cast(batch.column(1), field.data_type()).map_err(error::arrow)?;
        let metadata = metadata
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| error::invalid("expected metadata struct"))?;
        joined_rows += batch.num_rows();
        for row in 0..batch.num_rows() {
            let index = usize::try_from(indices.value(row))
                .map_err(|_| error::invalid("request ID overflow"))?;
            let object = keys
                .get(index)
                .ok_or_else(|| error::invalid("unknown request ID"))?;
            validation::validate_canonical_matches(matches.value(row), object)?;
            if selected.value(row) != indices.value(row) {
                let canonical = &keys[selected.value(row) as usize];
                eprintln!(
                    "BHL quarantined alternative {}: selected {}",
                    object.locator(),
                    canonical.locator()
                );
                continue;
            }
            emit(BhlPageMetadata {
                identity: BhlPageIdentity::parse(object.locator())?,
                object: object.clone(),
                metadata: metadata.slice(row, 1),
                missing: strings(batch.column(2).as_ref(), row)?,
                conflicts: strings(batch.column(3).as_ref(), row)?,
            })?;
        }
    }
    if joined_rows != keys.len() {
        return Err(error::invalid("unexpected joined page count"));
    }
    Ok(())
}

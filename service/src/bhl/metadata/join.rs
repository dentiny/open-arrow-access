use super::{BhlPageMetadata, BhlPageSelection, error};
use crate::bhl::{BhlPageIdentity, page_schema};
use arrow_array::{Array, ArrayRef, ListArray, RecordBatch, StringArray, StructArray, UInt64Array};
use open_arrow_access_core::{ObjectRef, Result};
use std::sync::Arc;

const SELECTION_BATCH_SIZE: usize = 8192;
const CANONICAL_IDS_COLUMN: usize = 4;

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
    mut emit: impl FnMut(BhlPageSelection) -> Result<()>,
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
    let mut parsed_identities = Vec::with_capacity(keys.len());
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
            parsed_identities.extend(identities);
        }
        appender.flush().map_err(error::duckdb)?;
    }
    super::scope::prepare(connection)?;
    let schema = page_schema();
    let field = schema.field_with_name("metadata").expect("fixed metadata");
    // Scope the source tables once, then keep wide joined records out of a
    // request-sized window/sort. Include every version of each page touched by
    // the batch, even when alternatives are far apart in the input.
    connection
        .execute_batch("ALTER TABLE bhl_selection RENAME TO bhl_all_selection")
        .map_err(error::duckdb)?;
    let mut joined_rows = 0;
    let mut canonical_pages = 0;
    for start in (0..keys.len()).step_by(SELECTION_BATCH_SIZE) {
        let end = start.saturating_add(SELECTION_BATCH_SIZE).min(keys.len());
        connection
            .execute_batch(&format!(
                include_str!("batch_selection.sql"),
                start = start,
                end = end,
            ))
            .map_err(error::duckdb)?;
        let mut statement = connection
            .prepare(include_str!("join.sql"))
            .map_err(error::duckdb)?;
        let _ = statement.stream_arrow([]).map_err(error::duckdb)?;
        while let Some(batch) = statement.step().map_err(error::duckdb)? {
            let batch = RecordBatch::from(&batch);
            let indices = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| error::invalid("expected request IDs"))?;
            let candidates = batch
                .column(CANONICAL_IDS_COLUMN)
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| error::invalid("expected canonical request lists"))?;
            // Enforce the fixed LargeUtf8/non-null contract on joined records only;
            // NULL cells in unrelated source rows must not fail this query.
            let metadata =
                arrow_cast::cast(batch.column(1), field.data_type()).map_err(error::arrow)?;
            let metadata = metadata
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| error::invalid("expected metadata struct"))?;
            for row in 0..batch.num_rows() {
                let index = usize::try_from(indices.value(row))
                    .map_err(|_| error::invalid("request ID overflow"))?;
                // Out-of-batch versions participate in canonicalization only.
                // Their own batch emits/reports them in original request order.
                if !(start..end).contains(&index) {
                    continue;
                }
                joined_rows += 1;
                let object = keys
                    .get(index)
                    .ok_or_else(|| error::invalid("unknown request ID"))?;
                let values = candidates.value(row);
                let ids = values
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| error::invalid("expected canonical request IDs"))?;
                if let Some(canonical_id) = ids.iter().next().flatten() {
                    let canonical_index = usize::try_from(canonical_id)
                        .map_err(|_| error::invalid("canonical request ID overflow"))?;
                    let canonical = keys
                        .get(canonical_index)
                        .ok_or_else(|| error::invalid("unknown canonical request ID"))?;
                    if canonical_index != index {
                        // Aliases are verified/reported by their canonical page.
                        // Stale sequence versions retain the existing skip policy.
                        if parsed_identities[canonical_index].sequence
                            != parsed_identities[index].sequence
                        {
                            eprintln!(
                                "BHL quarantined alternative {}: selected {}",
                                object.locator(),
                                canonical.locator()
                            );
                        }
                        continue;
                    }
                    canonical_pages += 1;
                }
                let objects = ids
                    .iter()
                    .map(|id| {
                        let id = usize::try_from(
                            id.ok_or_else(|| error::invalid("null canonical request ID"))?,
                        )
                        .map_err(|_| error::invalid("canonical request ID overflow"))?;
                        keys.get(id)
                            .cloned()
                            .ok_or_else(|| error::invalid("unknown canonical request ID"))
                    })
                    .collect::<Result<Vec<_>>>()?;
                emit(BhlPageSelection {
                    page: BhlPageMetadata {
                        identity: parsed_identities[index].clone(),
                        object: object.clone(),
                        metadata: metadata.slice(row, 1),
                        missing: strings(batch.column(2).as_ref(), row)?,
                        conflicts: strings(batch.column(3).as_ref(), row)?,
                    },
                    objects,
                })?;
            }
        }
    }
    if joined_rows != keys.len() {
        return Err(error::invalid("unexpected joined page count"));
    }
    if canonical_pages == 0 {
        return Err(error::invalid(format!(
            "no OCR page matches authoritative SequenceOrder; all {} selected objects quarantined",
            keys.len()
        ))
        .with_object(keys.first()));
    }
    Ok(())
}

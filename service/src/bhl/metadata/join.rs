use super::{BhlMetadata, BhlMetadataReport, error};
use crate::bhl::{BhlPageIdentity, page_schema};
use arrow_array::{Array, ListArray, StringArray, StructArray, UInt64Array};
use duckdb::params;
use open_arrow_access_core::{Error, ErrorKind, Result};
use std::{
    collections::HashMap,
    panic::{AssertUnwindSafe, catch_unwind},
};

type SelectedRows = HashMap<BhlPageIdentity, StructArray>;

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

pub(super) fn select(
    store: &BhlMetadata,
    keys: &[(String, BhlPageIdentity)],
) -> Result<(SelectedRows, BhlMetadataReport)> {
    let mut rows = HashMap::new();
    let mut report = BhlMetadataReport::default();
    if keys.is_empty() {
        return Ok((rows, report));
    }
    let mut connection = store
        .connection
        .lock()
        .map_err(|_| error::invalid("DuckDB metadata connection lock poisoned"))?;
    // The request table is private to this serialized transaction. Rollback on
    // any query/conversion failure keeps subsequent selections independent.
    let transaction = connection.transaction().map_err(error::duckdb)?;
    transaction
        .execute_batch("DELETE FROM bhl_selection;")
        .map_err(error::duckdb)?;
    {
        let mut appender = transaction
            .appender("bhl_selection")
            .map_err(error::duckdb)?;
        for (index, (_, key)) in keys.iter().enumerate() {
            appender
                .append_row(params![
                    index as u64,
                    key.entity_type,
                    key.entity_id,
                    key.page_id,
                    key.sequence
                ])
                .map_err(error::duckdb)?;
        }
        appender.flush().map_err(error::duckdb)?;
    }
    {
        let mut statement = transaction
            .prepare(include_str!("join.sql"))
            .map_err(error::duckdb)?;
        let mut batches = statement.stream_arrow([]).map_err(error::duckdb)?;
        loop {
            // The Arrow-58-compatible binding reports fetch/conversion failures
            // by panic; translate those into this API's explicit error channel.
            let batch = catch_unwind(AssertUnwindSafe(|| batches.next())).map_err(|_| {
                Error::new(
                    ErrorKind::Internal,
                    "bhl_metadata_duckdb",
                    "DuckDB Arrow fetch failed",
                )
            })?;
            let Some(batch) = batch else {
                break;
            };
            let indices = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| error::invalid("expected request IDs"))?;
            let schema = page_schema();
            let field = schema.field_with_name("metadata").expect("fixed metadata");
            // DuckDB exports VARCHAR as Utf8 and nullable children. Cast the
            // nested result to the existing LargeUtf8/nullability contract.
            let metadata =
                arrow_cast::cast(batch.column(1), field.data_type()).map_err(error::arrow)?;
            let metadata = metadata
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| error::invalid("expected metadata struct"))?;
            for row in 0..batch.num_rows() {
                let index = usize::try_from(indices.value(row))
                    .map_err(|_| error::invalid("request ID overflow"))?;
                let (path, identity) = keys
                    .get(index)
                    .ok_or_else(|| error::invalid("unknown request ID"))?;
                let missing = strings(batch.column(2).as_ref(), row)?;
                let conflicts = strings(batch.column(3).as_ref(), row)?;
                if !missing.is_empty() {
                    report.missing.insert(path.clone(), missing);
                }
                if !conflicts.is_empty() {
                    report.conflicts.insert(path.clone(), conflicts);
                }
                if rows
                    .insert(identity.clone(), metadata.slice(row, 1))
                    .is_some()
                {
                    return Err(error::invalid("DuckDB returned a duplicate selected page"));
                }
            }
        }
    }
    if rows.len() != keys.len() {
        return Err(error::invalid("DuckDB omitted selected pages"));
    }
    transaction
        .execute_batch("DELETE FROM bhl_selection;")
        .map_err(error::duckdb)?;
    transaction.commit().map_err(error::duckdb)?;
    Ok((rows, report))
}

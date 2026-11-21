use super::{BhlPageMetadata, error};
use crate::bhl::{BhlPageIdentity, page_schema};
use arrow_array::{Array, ListArray, RecordBatch, StringArray, StructArray, UInt64Array};
use duckdb::params;
use open_arrow_access_core::{ObjectRef, Result};

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
    connection: &duckdb::Connection,
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
        let mut appender = connection
            .appender("bhl_selection")
            .map_err(error::duckdb)?;
        for (index, object) in keys.iter().enumerate() {
            let key = BhlPageIdentity::parse(object.locator())?;
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
    let schema = page_schema();
    let field = schema.field_with_name("metadata").expect("fixed metadata");
    let mut statement = connection
        .prepare(include_str!("join.sql"))
        .map_err(error::duckdb)?;
    // Start streaming, then use the fallible step API: the older binding's
    // Arrow iterator panics on fetch errors instead of returning them.
    let _ = statement.stream_arrow([]).map_err(error::duckdb)?;
    let mut emitted = 0;
    while let Some(batch) = statement.step().map_err(error::duckdb)? {
        let batch = RecordBatch::from(&batch);
        let indices = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| error::invalid("expected request IDs"))?;
        // Match the fixed nested LargeUtf8/nullability contract.
        let metadata =
            arrow_cast::cast(batch.column(1), field.data_type()).map_err(error::arrow)?;
        let metadata = metadata
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| error::invalid("expected metadata struct"))?;
        for row in 0..batch.num_rows() {
            let index = usize::try_from(indices.value(row))
                .map_err(|_| error::invalid("request ID overflow"))?;
            let object = keys
                .get(index)
                .ok_or_else(|| error::invalid("unknown request ID"))?;
            emit(BhlPageMetadata {
                identity: BhlPageIdentity::parse(object.locator())?,
                object: object.clone(),
                metadata: metadata.slice(row, 1),
                missing: strings(batch.column(2).as_ref(), row)?,
                conflicts: strings(batch.column(3).as_ref(), row)?,
            })?;
            emitted += 1;
        }
    }
    if emitted != keys.len() {
        return Err(error::invalid("unexpected joined page count"));
    }
    Ok(())
}

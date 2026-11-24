//! Optional immutable lookup tables prepared once, before distributing a snapshot.
use super::{error, tables, validation};
use crate::duckdb::ManagedConnection;
use open_arrow_access_core::Result;

pub(super) fn available(connection: &ManagedConnection) -> Result<bool> {
    let exists: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables
         WHERE table_catalog=current_database() AND table_schema='main'
           AND table_name='__bhl_lookup')",
            [],
            |row| row.get(0),
        )
        .map_err(error::duckdb)?;
    if !exists {
        return Ok(false);
    }
    let version: (u64, Option<u64>) = connection
        .query_row(
            "SELECT count(*),min(version) FROM main.__bhl_lookup",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(error::duckdb)?;
    if version != (1, Some(1)) {
        return Err(error::invalid(
            "unsupported or incomplete BHL lookup profile",
        ));
    }
    for table in tables() {
        let columns = connection.table_columns(&format!("__bhl_lookup_{}", table.name))?;
        if columns.len() != table.fields.len() + 2 {
            return Err(error::invalid("invalid BHL lookup table schema"));
        }
        validation::validate_table_schema(&table, &columns[..table.fields.len()])?;
        let ordinal = &columns[table.fields.len()];
        let key = &columns[table.fields.len() + 1];
        if ordinal.name != "__source_rowid"
            || ordinal.data_type != "UBIGINT"
            || key.name != "__key"
            || key.data_type != "UBIGINT"
        {
            return Err(error::invalid("invalid BHL lookup key or source ordinal"));
        }
        let raw_count = connection.table_row_count(&table.name)?;
        let prepared_count = connection.table_row_count(&format!("__bhl_lookup_{}", table.name))?;
        if raw_count != prepared_count {
            return Err(error::invalid("incomplete BHL lookup table"));
        }
    }
    Ok(true)
}

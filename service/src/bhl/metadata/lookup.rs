//! Optional immutable lookup tables prepared once, before distributing a snapshot.
use super::{error, tables, validation};
use crate::duckdb::ManagedConnection;
use open_arrow_access_core::Result;

/// Select a fixed query script after validating the optional immutable profile.
/// The database handle retains this choice instead of inspecting it on each read.
pub(super) fn query_sql(connection: &ManagedConnection) -> Result<&'static str> {
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
        return Ok(include_str!("raw_views.sql"));
    }
    let (count, version): (u64, Option<u64>) = connection
        .query_row(
            "SELECT count(*),min(version) FROM main.__bhl_lookup",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(error::duckdb)?;
    validation::validate_lookup_header(count, version)?;
    for table in tables() {
        let columns = connection.table_columns(&format!("__bhl_lookup_{}", table.name))?;
        validation::validate_lookup_schema(&table, &columns)?;
        let raw_count = connection.table_row_count(&table.name)?;
        let prepared_count = connection.table_row_count(&format!("__bhl_lookup_{}", table.name))?;
        validation::validate_lookup_rows(raw_count, prepared_count)?;
    }
    Ok(include_str!("lookup_views.sql"))
}

//! Validation of persistent metadata databases and stored source tables.
use super::{BhlMetadataInfo, database::DATABASE_VERSION, error, tables};
use duckdb::Connection;
use open_arrow_access_core::Result;
use std::path::Path;

pub(super) fn database_destination(path: &Path) -> Result<()> {
    if path.try_exists().map_err(error::database_io)? {
        return Err(error::invalid("database path already exists"));
    }
    Ok(())
}

pub(super) fn database_header(count: u64, version: u64) -> Result<()> {
    if count != 1 || version != DATABASE_VERSION {
        return Err(error::invalid(
            "unsupported BHL metadata database version/header",
        ));
    }
    Ok(())
}

pub(super) fn database_sources(connection: &Connection, info: &BhlMetadataInfo) -> Result<()> {
    let tables = tables();
    if info.sources.len() != tables.len() {
        return Err(error::invalid("incomplete database source manifest"));
    }
    for table in tables {
        let mut query = connection
            .prepare(&format!("DESCRIBE main.\"{}\"", table.name))
            .map_err(error::duckdb)?;
        let actual = query
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(error::duckdb)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(error::duckdb)?;
        let expected = table
            .fields
            .iter()
            .map(|field| (field.name().clone(), "VARCHAR".to_owned()))
            .collect::<Vec<_>>();
        if actual != expected || !info.sources.contains_key(&table.name) {
            return Err(error::invalid("database source schema mismatch"));
        }
        let rows: u64 = connection
            .query_row(
                &format!("SELECT count(*) FROM main.\"{}\"", table.name),
                [],
                |row| row.get(0),
            )
            .map_err(error::duckdb)?;
        if rows != info.sources[&table.name].rows {
            return Err(error::invalid("database source row count mismatch"));
        }
    }
    Ok(())
}

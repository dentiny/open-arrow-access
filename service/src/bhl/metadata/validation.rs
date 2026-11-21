//! Validation of prepared metadata snapshot inputs and stored source tables.
use super::{BhlMetadataInfo, error, tables};
use duckdb::Connection;
use open_arrow_access_core::Result;
use std::path::Path;

pub(super) const SNAPSHOT_VERSION: u64 = 1;

pub(super) fn snapshot_path(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| error::invalid("snapshot path must be UTF-8"))
}

pub(super) fn completed_import(connection: &Connection) -> Result<()> {
    let imported: usize = connection.query_row(
        "SELECT count(*) FROM information_schema.tables WHERE table_catalog = current_database() AND table_schema = 'main' AND table_type = 'BASE TABLE'",
        [], |row| row.get(0),
    ).map_err(error::duckdb)?;
    if imported != tables().len() {
        return Err(error::invalid(
            "save_snapshot requires a completed source import",
        ));
    }
    Ok(())
}

pub(super) fn snapshot(connection: &Connection) -> Result<BhlMetadataInfo> {
    let (count, version, info): (u64, u64, String) = connection
        .query_row(
            "SELECT count(*), min(version), min(info) FROM bhl_source.__bhl_snapshot",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(error::duckdb)?;
    if count != 1 || version != SNAPSHOT_VERSION {
        return Err(error::invalid(
            "unsupported BHL metadata snapshot version/header",
        ));
    }
    let info: BhlMetadataInfo = serde_json::from_str(&info).map_err(error::json)?;
    let tables = tables();
    if info.sources.len() != tables.len() {
        return Err(error::invalid("incomplete snapshot source manifest"));
    }
    for table in tables {
        let mut query = connection
            .prepare(&format!("DESCRIBE bhl_source.\"{}\"", table.name))
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
            return Err(error::invalid("snapshot source schema mismatch"));
        }
        let rows: u64 = connection
            .query_row(
                &format!("SELECT count(*) FROM bhl_source.\"{}\"", table.name),
                [],
                |row| row.get(0),
            )
            .map_err(error::duckdb)?;
        if rows != info.sources[&table.name].rows {
            return Err(error::invalid("snapshot source row count mismatch"));
        }
    }
    Ok(info)
}

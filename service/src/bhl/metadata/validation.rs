//! Validation of prepared metadata snapshot inputs and stored source tables.
use super::{BhlMetadataInfo, error, tables};
use duckdb::Connection;
use open_arrow_access_core::Result;
use std::path::Path;

pub(super) const SNAPSHOT_VERSION: u64 = 1;

pub(super) fn snapshot_destination(path: &Path) -> Result<()> {
    if path.try_exists().map_err(error::snapshot_io)? {
        return Err(error::invalid("snapshot path already exists"));
    }
    Ok(())
}

pub(super) fn snapshot(connection: &Connection) -> Result<BhlMetadataInfo> {
    let (count, version, info): (u64, u64, String) = connection
        .query_row(
            "SELECT count(*), min(version), min(info) FROM main.__bhl_snapshot",
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
            return Err(error::invalid("snapshot source schema mismatch"));
        }
        let rows: u64 = connection
            .query_row(
                &format!("SELECT count(*) FROM main.\"{}\"", table.name),
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

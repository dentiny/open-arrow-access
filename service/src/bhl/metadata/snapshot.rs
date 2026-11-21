//! Explicit, immutable metadata reuse across independently scheduled page jobs.
use super::{BhlMetadata, BhlMetadataInfo, error, tables};
use duckdb::{Config, Connection, params};
use open_arrow_access_core::{Error, ErrorKind, Result};
use std::{path::Path, sync::Mutex};

const VERSION: u64 = 1;

fn io(error: std::io::Error) -> Error {
    Error::new(
        ErrorKind::Io,
        "bhl_metadata_snapshot",
        "metadata snapshot I/O failed",
    )
    .with_source(error)
}

impl BhlMetadata {
    /// Save a completed import for reuse by separate page jobs. Never overwrites
    /// an existing path. Publish only after all data and provenance are committed.
    /// The caller owns snapshot naming, distribution and source freshness.
    pub fn save_snapshot(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        let stage = tempfile::Builder::new()
            .prefix(".bhl-metadata-")
            .tempdir_in(
                path.parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(Path::new(".")),
            )
            .map_err(io)?;
        let file = stage.path().join("metadata.duckdb");
        let filename = file
            .to_str()
            .ok_or_else(|| error::invalid("snapshot path must be UTF-8"))?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| error::invalid("metadata connection lock poisoned"))?;
        let imported: usize = connection.query_row(
            "SELECT count(*) FROM information_schema.tables WHERE table_catalog = current_database() AND table_schema = 'main' AND table_type = 'BASE TABLE'",
            [], |row| row.get(0),
        ).map_err(error::duckdb)?;
        if imported != tables().len() {
            return Err(error::invalid(
                "save_snapshot requires a completed source import",
            ));
        }
        let source: String = connection
            .query_row("SELECT current_database()", [], |row| row.get(0))
            .map_err(error::duckdb)?;
        connection
            .execute_batch(&format!(
                "ATTACH '{}' AS bhl_export",
                filename.replace('\'', "''")
            ))
            .map_err(error::duckdb)?;
        let copied = (|| {
            connection.execute_batch(&format!(
                "COPY FROM DATABASE \"{}\" TO bhl_export; CREATE TABLE bhl_export.__bhl_snapshot (version UBIGINT, info VARCHAR);",
                source.replace('"', "\"\"")
            )).map_err(error::duckdb)?;
            let info = serde_json::to_string(&self.info).map_err(error::json)?;
            connection
                .execute(
                    "INSERT INTO bhl_export.__bhl_snapshot VALUES (?, ?)",
                    params![VERSION, info],
                )
                .map_err(error::duckdb)?;
            connection
                .execute_batch("CHECKPOINT bhl_export")
                .map_err(error::duckdb)?;
            Ok(())
        })();
        let detached = connection
            .execute_batch("DETACH bhl_export")
            .map_err(error::duckdb);
        copied?;
        detached?;
        // The stage is on the target filesystem; hard_link publishes atomically
        // without replacing an existing snapshot, including in a writer race.
        std::fs::hard_link(&file, path).map_err(io)?;
        Ok(())
    }

    /// Open a distributed snapshot read-only, without acquiring any AWS tables.
    /// Temporary per-query selections remain private to cloned connections.
    pub fn open_snapshot(path: impl AsRef<Path>) -> Result<Self> {
        let filename = path
            .as_ref()
            .to_str()
            .ok_or_else(|| error::invalid("snapshot path must be UTF-8"))?;
        let config = Config::default()
            .with("enable_external_file_cache", "false")
            .map_err(error::duckdb)?;
        // Keep query selections in writable private memory; source data itself is
        // attached read-only and is never copied into another in-memory database.
        let connection = Connection::open_in_memory_with_flags(config).map_err(error::duckdb)?;
        connection
            .execute_batch(&format!(
                "ATTACH '{}' AS bhl_source (READ_ONLY)",
                filename.replace('\'', "''")
            ))
            .map_err(error::duckdb)?;
        let (count, version, info): (u64, u64, String) = connection
            .query_row(
                "SELECT count(*), min(version), min(info) FROM bhl_source.__bhl_snapshot",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(error::duckdb)?;
        if count != 1 || version != VERSION {
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
            connection.execute_batch(&format!(
                "CREATE VIEW \"{}\" AS SELECT * FROM bhl_source.\"{}\"; CREATE VIEW bhl_{} AS SELECT * FROM bhl_source.bhl_{};",
                table.name, table.name, table.name, table.name
            )).map_err(error::duckdb)?;
            let mut query = connection
                .prepare(&format!("DESCRIBE \"{}\"", table.name))
                .map_err(error::duckdb)?;
            let actual = query
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(error::duckdb)?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(error::duckdb)?;
            let mut expected = vec![
                ("__key".to_owned(), "UBIGINT".to_owned()),
                ("__ordinal".to_owned(), "UBIGINT".to_owned()),
            ];
            expected.extend(
                table
                    .fields
                    .iter()
                    .map(|field| (field.name().clone(), "VARCHAR".to_owned())),
            );
            if actual != expected || !info.sources.contains_key(&table.name) {
                return Err(error::invalid("snapshot source schema mismatch"));
            }
            let rows: u64 = connection
                .query_row(
                    &format!("SELECT count(*) FROM \"{}\"", table.name),
                    [],
                    |row| row.get(0),
                )
                .map_err(error::duckdb)?;
            if rows != info.sources[&table.name].rows {
                return Err(error::invalid("snapshot source row count mismatch"));
            }
        }
        connection
            .execute_batch("CREATE MACRO bhl_id(value) AS bhl_source.bhl_id(value)")
            .map_err(error::duckdb)?;
        Ok(Self {
            connection: Mutex::new(connection),
            info,
        })
    }
}

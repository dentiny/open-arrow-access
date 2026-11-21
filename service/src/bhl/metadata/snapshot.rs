//! Explicit, immutable metadata reuse across independently scheduled page jobs.
use super::{BhlMetadata, error, tables, validation};
use duckdb::{Config, Connection, params};
use open_arrow_access_core::Result;
use std::{path::Path, sync::Mutex};

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
            .map_err(error::snapshot_io)?;
        let file = stage.path().join("metadata.duckdb");
        let filename = validation::snapshot_path(&file)?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| error::invalid("metadata connection lock poisoned"))?;
        validation::completed_import(&connection)?;
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
                    params![validation::SNAPSHOT_VERSION, info],
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
        std::fs::hard_link(&file, path).map_err(error::snapshot_io)?;
        Ok(())
    }

    /// Open a distributed snapshot read-only, without acquiring any AWS tables.
    /// Temporary per-query selections remain private to cloned connections.
    pub fn open_snapshot(path: impl AsRef<Path>) -> Result<Self> {
        let filename = validation::snapshot_path(path.as_ref())?;
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
        let info = validation::snapshot(&connection)?;
        for table in tables() {
            connection.execute_batch(&format!(
                "CREATE VIEW \"{}\" AS SELECT * FROM bhl_source.\"{}\"; CREATE VIEW bhl_{} AS SELECT * FROM bhl_source.bhl_{};",
                table.name, table.name, table.name, table.name
            )).map_err(error::duckdb)?;
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

//! Explicit, immutable metadata reuse across independently scheduled page jobs.
use super::{BhlMetadata, BhlMetadataInfo, error, validation};
use duckdb::{Config, Connection, params};
use open_arrow_access_core::{ByteAccessor, Result};
use std::{path::Path, sync::Mutex};

fn open_read_only(path: impl AsRef<Path>) -> Result<Connection> {
    let config = Config::default()
        .with("enable_external_file_cache", "false")
        .map_err(error::duckdb)?
        .with("access_mode", "READ_ONLY")
        .map_err(error::duckdb)?;
    Connection::open_with_flags(path, config).map_err(error::duckdb)
}

fn read_info(connection: &Connection) -> Result<BhlMetadataInfo> {
    let (count, version, info): (u64, u64, String) = connection
        .query_row(
            "SELECT count(*), min(version), min(info) FROM main.__bhl_snapshot",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(error::duckdb)?;
    validation::snapshot_header(count, version)?;
    serde_json::from_str(&info).map_err(error::json)
}

impl BhlMetadata {
    /// Import directly into a persistent database for reuse by separate page jobs.
    /// Publish only after all data and provenance are committed and the database
    /// is closed. Never overwrites an existing path. The caller owns snapshot
    /// naming, distribution and source freshness.
    pub async fn prepare_snapshot<A: ByteAccessor + 'static>(
        storage: A,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        let path = path.as_ref().to_path_buf();
        validation::snapshot_destination(&path)?;
        let stage = tempfile::Builder::new()
            .prefix(".bhl-metadata-")
            .tempdir_in(
                path.parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(Path::new(".")),
            )
            .map_err(error::snapshot_io)?;
        let file = stage.path().join("metadata.duckdb");
        let config = Config::default()
            .with("enable_external_file_cache", "false")
            .map_err(error::duckdb)?;
        let connection = Connection::open_with_flags(&file, config).map_err(error::duckdb)?;
        let metadata = Self::import(storage, connection).await?;
        tokio::task::spawn_blocking(move || {
            let connection = metadata.connection.into_inner().map_err(error::metadata_lock)?;
            let info = serde_json::to_string(&metadata.info).map_err(error::json)?;
            connection
                .execute_batch(
                    "BEGIN TRANSACTION; CREATE TABLE __bhl_snapshot (version UBIGINT, info VARCHAR);",
                )
                .map_err(error::duckdb)?;
            connection
                .execute(
                    "INSERT INTO __bhl_snapshot VALUES (?, ?)",
                    params![validation::SNAPSHOT_VERSION, info],
                )
                .map_err(error::duckdb)?;
            connection
                .execute_batch("COMMIT; CHECKPOINT")
                .map_err(error::duckdb)?;
            connection.close().map_err(|(_, source)| error::duckdb(source))?;
            // The stage is on the target filesystem; hard_link publishes atomically
            // without replacing an existing snapshot, including in a writer race.
            std::fs::hard_link(&file, path).map_err(error::snapshot_io)?;
            drop(stage);
            Ok(())
        })
        .await
        .map_err(error::task)?
    }

    /// Open the persistent database read-only, without acquiring any AWS tables.
    /// Temporary per-query selections remain private to cloned connections.
    pub fn open_snapshot(path: impl AsRef<Path>) -> Result<Self> {
        let connection = open_read_only(path)?;
        let info = read_info(&connection)?;
        validation::snapshot_sources(&connection, &info)?;
        Ok(Self {
            connection: Mutex::new(connection),
            info,
        })
    }
}

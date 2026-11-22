//! Persistent DuckDB storage for reusable BHL metadata.
use super::{BhlMetadata, BhlMetadataInfo, error, tables, validation};
use duckdb::{Config, Connection, params};
use open_arrow_access_core::{ByteAccessor, Result};
use std::{path::Path, sync::Mutex};

pub(super) const DATABASE_VERSION: u64 = 1;

fn open_database(path: impl AsRef<Path>) -> Result<Connection> {
    let config = Config::default()
        .with("enable_external_file_cache", "false")
        .map_err(error::duckdb)?
        .with("access_mode", "READ_WRITE")
        .map_err(error::duckdb)?;
    Connection::open_with_flags(path, config).map_err(error::duckdb)
}

fn read_info(connection: &Connection) -> Result<BhlMetadataInfo> {
    let (count, version, info): (u64, u64, String) = connection
        .query_row(
            "SELECT count(*), min(version), min(info) FROM main.__bhl_metadata",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(error::duckdb)?;
    validation::validate_database_header(count, version)?;
    serde_json::from_str(&info).map_err(error::json)
}

fn check_sources(connection: &Connection, info: &BhlMetadataInfo) -> Result<()> {
    let tables = tables();
    validation::validate_database_manifest(info, tables.len())?;
    for table in tables {
        let mut query = connection
            .prepare(&format!("DESCRIBE main.\"{}\"", table.name))
            .map_err(error::duckdb)?;
        let columns = query
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(error::duckdb)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(error::duckdb)?;
        validation::validate_database_source_schema(&table, &columns, info)?;
        let rows: u64 = connection
            .query_row(
                &format!("SELECT count(*) FROM main.\"{}\"", table.name),
                [],
                |row| row.get(0),
            )
            .map_err(error::duckdb)?;
        validation::validate_database_source_rows(rows, info.sources[&table.name].rows)?;
    }
    Ok(())
}

impl BhlMetadata {
    /// Create a persistent metadata database by importing the 13 source tables.
    /// Publish only after all data and provenance are committed and the database
    /// is closed. Never overwrites an existing path. The caller owns database
    /// naming, distribution and source freshness.
    pub async fn create<A: ByteAccessor + 'static>(
        storage: A,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        let path = path.as_ref().to_path_buf();
        validation::validate_database_destination(path.try_exists().map_err(error::database_io)?)?;
        let stage = tempfile::Builder::new()
            .prefix(".bhl-metadata-")
            .tempdir_in(
                path.parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(Path::new(".")),
            )
            .map_err(error::database_io)?;
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
                    "BEGIN TRANSACTION; CREATE TABLE __bhl_metadata (version UBIGINT, info VARCHAR);",
                )
                .map_err(error::duckdb)?;
            connection
                .execute(
                    "INSERT INTO __bhl_metadata VALUES (?, ?)",
                    params![DATABASE_VERSION, info],
                )
                .map_err(error::duckdb)?;
            connection
                .execute_batch("COMMIT; CHECKPOINT")
                .map_err(error::duckdb)?;
            connection.close().map_err(|(_, source)| error::duckdb(source))?;
            // The stage is on the target filesystem; hard_link publishes atomically
            // without replacing an existing database, including in a writer race.
            std::fs::hard_link(&file, path).map_err(error::database_io)?;
            drop(stage);
            Ok(())
        })
        .await
        .map_err(error::task)?
    }

    /// Open the persistent database read-write, without acquiring any AWS tables.
    /// Temporary per-query selections remain private to cloned connections.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let connection = open_database(path)?;
        let info = read_info(&connection)?;
        check_sources(&connection, &info)?;
        Ok(Self {
            connection: Mutex::new(connection),
            info,
        })
    }
}

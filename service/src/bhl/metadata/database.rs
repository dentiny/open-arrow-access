//! Persistent DuckDB storage for reusable BHL metadata.
use super::{BhlMetadata, BhlMetadataInfo, SourceInfo, error, source, tables, validation};
use crate::duckdb::{DuckDbDatabase, ManagedConnection};
use ::duckdb::{Connection, params};
use open_arrow_access_core::{ByteAccessor, Result};
use std::{collections::BTreeMap, path::Path, sync::Mutex};

pub(super) const DATABASE_VERSION: u64 = 1;

fn read_info(connection: &Connection) -> Result<BhlMetadataInfo> {
    let (count, version, info): (u64, u64, String) = connection
        .query_row(
            "SELECT count(*), min(version), min(info) FROM main.__bhl_metadata",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(error::duckdb)?;
    validation::validate_database_header(count, version)?;
    let info: serde_json::Value = serde_json::from_str(&info).map_err(error::json)?;
    validation::validate_managed_source_checksums(&info)?;
    serde_json::from_value(info).map_err(error::json)
}

fn check_sources(connection: &ManagedConnection, info: &BhlMetadataInfo) -> Result<()> {
    let tables = tables();
    validation::validate_database_manifest(info, tables.len())?;
    for table in tables {
        let columns = connection.table_columns(&table.name)?;
        validation::validate_database_source_schema(&table, &columns, info)?;
        let rows = connection.table_row_count(&table.name)?;
        validation::validate_database_source_rows(rows, info.sources[&table.name].rows)?;
    }
    Ok(())
}

impl BhlMetadata {
    /// Create a persistent metadata database by importing the 13 source tables.
    /// Write directly to the requested path, rejecting an existing destination.
    /// Await successful completion before opening readers: data and provenance
    /// are committed and all write connections are closed before returning.
    /// A failed import may leave an incomplete file; discard it before retrying.
    pub async fn create<A: ByteAccessor + 'static>(
        storage: A,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        let path = path.as_ref().to_path_buf();
        validation::validate_database_destination(path.try_exists().map_err(error::database_io)?)?;
        let connection = ManagedConnection::open(&path, false)?;
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
            connection.close()
        })
        .await
        .map_err(error::task)?
    }

    /// Open the persistent database read-only, without acquiring any AWS tables.
    /// Temporary per-query selections remain private to cloned connections.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let connection = ManagedConnection::open(path, true)?;
        let info = read_info(&connection)?;
        check_sources(&connection, &info)?;
        Ok(Self {
            connection: Mutex::new(connection),
            info,
        })
    }

    /// Open a caller-created DuckDB file containing the 13 BHL source tables.
    /// No TSV acquisition, table copying, or private manifest is required.
    ///
    /// The file is opened read-only. All required columns must appear in source
    /// order as VARCHAR, preserving empty strings. Query helpers are temporary
    /// and private to each query connection. `snapshot_id` must identify this
    /// immutable database (for example, its SHA-256 computed once by the caller).
    /// It is not presented as a checksum of the original TSV rows.
    pub fn open_tables(path: impl AsRef<Path>, snapshot_id: impl Into<String>) -> Result<Self> {
        Self::from_tables_database(DuckDbDatabase::open(path)?, snapshot_id)
    }

    /// Bind a shared local, remote, or staged DuckDB database to BHL tables.
    /// File loading, credentials and source lifetime belong to the shared module.
    pub fn from_tables_database(
        database: DuckDbDatabase,
        snapshot_id: impl Into<String>,
    ) -> Result<Self> {
        let snapshot_id = snapshot_id.into();
        validation::validate_database_snapshot_id(&snapshot_id)?;
        let (connection, location) = database.into_connection();
        let mut sources = BTreeMap::new();
        for table in tables() {
            let columns = connection.table_columns(&table.name)?;
            validation::validate_table_schema(&table, &columns)?;
            let rows = connection.table_row_count(&table.name)?;
            sources.insert(
                table.name.clone(),
                SourceInfo {
                    path: format!("{location}#table={}", table.name),
                    rows,
                    sha256: String::new(),
                },
            );
        }
        source::create_id_macro(&connection, true)?;
        let invalid: u64 = connection
            .query_row(
                "SELECT count(*) FROM main.title WHERE bhl_id(TitleID) IS NULL",
                [],
                |row| row.get(0),
            )
            .map_err(error::duckdb)?;
        let duplicates: u64 = connection.query_row(
            "SELECT count(*) FROM (SELECT bhl_id(TitleID) FROM main.title GROUP BY bhl_id(TitleID) HAVING count(*) > 1)",
            [], |row| row.get(0),
        ).map_err(error::duckdb)?;
        if invalid != 0 || duplicates != 0 {
            return Err(error::invalid("invalid or duplicate numeric key in title"));
        }
        Ok(Self {
            connection: Mutex::new(connection),
            info: BhlMetadataInfo {
                source_root: super::super::OPEN_DATA_BUCKET_URL.to_owned(),
                fingerprint: snapshot_id,
                sources,
            },
        })
    }
}

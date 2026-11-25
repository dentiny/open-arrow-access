//! Keep scratch storage alive for every DuckDB connection, including import workers.
use super::{CachePolicy, error};
use ::duckdb::{Config, Connection};
use open_arrow_access_core::Result;
use std::{ops::Deref, path::Path, sync::Arc};

/// A column described by DuckDB, retaining its SQL type name.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TableColumn {
    pub(crate) name: String,
    pub(crate) data_type: String,
}

pub(crate) struct ManagedConnection {
    // Fields drop in declaration order: close DuckDB before removing its files.
    connection: Connection,
    directory: Arc<tempfile::TempDir>,
    cache_policy: CachePolicy,
}

impl ManagedConnection {
    fn config() -> Result<(Config, Arc<tempfile::TempDir>)> {
        let directory = Arc::new(tempfile::tempdir().map_err(error::scratch)?);
        let config = Config::default()
            .with("enable_external_file_cache", "false")
            .map_err(error::database)?
            .with(
                "temp_directory",
                directory.path().join("spill").to_string_lossy(),
            )
            .map_err(error::database)?;
        Ok((config, directory))
    }

    pub(crate) fn in_memory() -> Result<Self> {
        let (config, directory) = Self::config()?;
        let connection = Connection::open_in_memory_with_flags(config).map_err(error::database)?;
        Ok(Self {
            connection,
            directory,
            cache_policy: CachePolicy::on_disk(),
        })
    }

    pub(crate) fn open(path: impl AsRef<Path>, read_only: bool) -> Result<Self> {
        let (mut config, directory) = Self::config()?;
        if read_only {
            config = config
                .with("access_mode", "READ_ONLY")
                .map_err(error::database)?;
        }
        let connection = Connection::open_with_flags(path, config).map_err(error::database)?;
        Ok(Self {
            connection,
            directory,
            cache_policy: CachePolicy::on_disk(),
        })
    }

    /// Initialize secrets before attaching an existing local or remote database.
    /// The script is trusted caller SQL, executed once per database instance.
    pub(crate) fn connect(
        location: &str,
        init_sql: Option<&str>,
        cache_policy: CachePolicy,
    ) -> Result<Self> {
        let connection = Self::in_memory_with_init(init_sql, cache_policy)?;
        connection.configure_remote_cache()?;
        let location = location.replace('\'', "''");
        connection
            .execute_batch(&format!(
                "ATTACH '{location}' AS source_database (READ_ONLY); USE source_database;"
            ))
            .map_err(|_| error::attach())?;
        Ok(connection)
    }

    pub(crate) fn in_memory_with_init(
        init_sql: Option<&str>,
        cache_policy: CachePolicy,
    ) -> Result<Self> {
        cache_policy.sql()?;
        let mut connection = Self::in_memory()?;
        connection.cache_policy = cache_policy;
        if let Some(sql) = init_sql {
            // DuckDB errors may echo SQL containing credentials; do not retain them.
            connection
                .execute_batch(sql)
                .map_err(|_| error::init_setup())?;
        }
        Ok(connection)
    }

    /// The same scratch owner keeps spill files and cached remote blocks alive
    /// until the final cloned connection closes. No full database copy is made.
    pub(crate) fn configure_remote_cache(&self) -> Result<()> {
        let cache = self.directory.path().join("http-cache");
        std::fs::create_dir_all(&cache).map_err(error::scratch)?;
        // Global defaults reach clones; local settings override initialization SQL.
        let policy = self.cache_policy.sql()?;
        self.connection
            .execute_batch(&format!(
                "FORCE INSTALL cache_httpfs FROM community;
                 LOAD cache_httpfs;
                 {policy}
                 SET GLOBAL cache_httpfs_cache_directories_config = '';
                 SET cache_httpfs_cache_directories_config = '';
                 SET GLOBAL cache_httpfs_cache_directory = '{}';
                 SET cache_httpfs_cache_directory = '{}';",
                cache.to_string_lossy().replace('\'', "''"),
                cache.to_string_lossy().replace('\'', "''")
            ))
            .map_err(error::cache_setup)
    }

    /// Describe a caller-owned table in the main schema without copying it.
    pub(crate) fn table_columns(&self, table: &str) -> Result<Vec<TableColumn>> {
        let table = table.replace('"', "\"\"");
        let mut statement = self
            .connection
            .prepare(&format!("DESCRIBE main.\"{table}\""))
            .map_err(error::database)?;
        statement
            .query_map([], |row| {
                Ok(TableColumn {
                    name: row.get(0)?,
                    data_type: row.get(1)?,
                })
            })
            .map_err(error::database)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(error::database)
    }

    /// Read a main-schema table's row count without materializing its rows.
    pub(crate) fn table_row_count(&self, table: &str) -> Result<u64> {
        let table = table.replace('"', "\"\"");
        self.connection
            .query_row(
                &format!("SELECT count(*) FROM main.\"{table}\""),
                [],
                |row| row.get(0),
            )
            .map_err(error::database)
    }

    /// Store both the database and its spill files on disposable local disk.
    pub(crate) fn temporary() -> Result<Self> {
        let (config, directory) = Self::config()?;
        let connection =
            Connection::open_with_flags(directory.path().join("snapshot.duckdb"), config)
                .map_err(error::database)?;
        Ok(Self {
            connection,
            directory,
            cache_policy: CachePolicy::on_disk(),
        })
    }

    pub(crate) fn try_clone(&self) -> Result<Self> {
        let search_path: String = self
            .connection
            .query_row("SELECT current_setting('search_path')", [], |row| {
                row.get(0)
            })
            .map_err(error::database)?;
        let connection = self.connection.try_clone().map_err(error::database)?;
        // The qualified search path preserves both catalog and schema selection,
        // which DuckDB resets when cloning a connection.
        connection
            .execute_batch(&format!(
                "SET search_path = '{}';",
                search_path.replace('\'', "''")
            ))
            .map_err(error::database)?;
        Ok(Self {
            connection,
            directory: self.directory.clone(),
            cache_policy: self.cache_policy,
        })
    }

    pub(crate) fn close(self) -> Result<()> {
        let result = self
            .connection
            .close()
            .map_err(|(_, source)| error::database(source));
        drop(self.directory);
        result
    }
}

impl Deref for ManagedConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

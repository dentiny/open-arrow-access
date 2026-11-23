//! Keep scratch storage alive for every DuckDB connection, including import workers.
use super::error;
use ::duckdb::{Config, Connection};
use open_arrow_access_core::Result;
use std::{ops::Deref, path::Path, sync::Arc};

pub(crate) struct ManagedConnection {
    // Fields drop in declaration order: close DuckDB before removing its files.
    connection: Connection,
    directory: Arc<tempfile::TempDir>,
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
        })
    }

    /// Initialize secrets before attaching an existing local or remote database.
    /// The script is trusted caller SQL, executed once per database instance.
    pub(crate) fn connect(location: &str, secret_sql_file: Option<&Path>) -> Result<Self> {
        let connection = Self::in_memory()?;
        if let Some(path) = secret_sql_file {
            let sql = std::fs::read_to_string(path).map_err(|_| error::secret_setup())?;
            // DuckDB errors may echo SQL containing credentials; do not retain them.
            connection
                .execute_batch(&sql)
                .map_err(|_| error::secret_setup())?;
        }
        connection.configure_remote_cache()?;
        let location = location.replace('\'', "''");
        connection
            .execute_batch(&format!(
                "ATTACH '{location}' AS source_database (READ_ONLY); USE source_database;"
            ))
            .map_err(|_| error::attach())?;
        Ok(connection)
    }

    /// The same scratch owner keeps spill files and cached remote blocks alive
    /// until the final cloned connection closes. No full database copy is made.
    fn configure_remote_cache(&self) -> Result<()> {
        let cache = self.directory.path().join("http-cache");
        std::fs::create_dir_all(&cache).map_err(error::scratch)?;
        self.connection
            .execute_batch(&format!(
                "LOAD cache_httpfs;
                 SET cache_httpfs_type = 'on_disk';
                 SET cache_httpfs_cache_directories_config = '';
                 SET cache_httpfs_cache_directory = '{}';",
                cache.to_string_lossy().replace('\'', "''")
            ))
            .map_err(error::cache_setup)
    }

    /// Describe a caller-owned table in the main schema without copying it.
    pub(crate) fn table_columns(&self, table: &str) -> Result<Vec<(String, String)>> {
        let table = table.replace('"', "\"\"");
        let mut statement = self
            .connection
            .prepare(&format!("DESCRIBE main.\"{table}\""))
            .map_err(error::database)?;
        statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
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
        })
    }

    pub(crate) fn try_clone(&self) -> Result<Self> {
        let (database, search_path): (String, String) = self
            .connection
            .query_row(
                "SELECT current_database(), current_setting('search_path')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(error::database)?;
        let connection = self.connection.try_clone().map_err(error::database)?;
        // DuckDB clones reset catalog and schema selection. Restore the complete
        // search path so unqualified names resolve exactly as on the source.
        connection
            .execute_batch(&format!(
                "USE \"{}\"; SET search_path = '{}';",
                database.replace('"', "\"\""),
                search_path.replace('\'', "''")
            ))
            .map_err(error::database)?;
        Ok(Self {
            connection,
            directory: self.directory.clone(),
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

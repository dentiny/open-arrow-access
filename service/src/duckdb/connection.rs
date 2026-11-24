//! Keep scratch storage alive for every DuckDB connection, including import workers.
use super::error;
use ::duckdb::{Config, Connection};
use open_arrow_access_core::Result;
use std::{
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};

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
    pub(crate) fn connect(location: &str, init_sql: Option<&str>) -> Result<Self> {
        Self::connect_with_cache_directory(location, init_sql, None)
    }

    pub(crate) fn connect_with_cache_directory(
        location: &str,
        init_sql: Option<&str>,
        cache_directory: Option<&Path>,
    ) -> Result<Self> {
        let connection = Self::in_memory()?;
        if let Some(sql) = init_sql {
            // DuckDB errors may echo SQL containing credentials; do not retain them.
            connection
                .execute_batch(sql)
                .map_err(|_| error::init_setup())?;
        }
        connection.configure_remote_cache(cache_directory)?;
        let location = location.replace('\'', "''");
        connection
            .execute_batch(&format!(
                "ATTACH '{location}' AS source_database (READ_ONLY); USE source_database;"
            ))
            .map_err(|_| error::attach())?;
        Ok(connection)
    }

    fn cache_directory(&self, requested: Option<&Path>) -> Result<PathBuf> {
        let cache = requested
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.directory.path().join("http-cache"));
        std::fs::create_dir_all(&cache).map_err(error::scratch)?;
        // Resolve relative paths before passing them to DuckDB, retaining the
        // caller's directory without transferring ownership to the scratch guard.
        std::fs::canonicalize(cache).map_err(error::scratch)
    }

    /// Spill is always private. The default cache shares its scratch lifetime;
    /// an explicitly supplied cache is caller-owned and survives all clones.
    fn configure_remote_cache(&self, requested: Option<&Path>) -> Result<()> {
        let cache = self.cache_directory(requested)?;
        self.connection
            .execute_batch(&format!(
                "FORCE INSTALL cache_httpfs FROM community;
                 LOAD cache_httpfs;
                 SET cache_httpfs_type = 'on_disk';
                 SET cache_httpfs_cache_directories_config = '';
                 SET cache_httpfs_cache_directory = '{}';",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_cache_lives_until_the_last_clone_closes() {
        let connection = ManagedConnection::in_memory().unwrap();
        let cache = connection.cache_directory(None).unwrap();
        std::fs::write(cache.join("block"), b"cached bytes").unwrap();
        let clone = connection.try_clone().unwrap();
        drop(connection);
        assert_eq!(std::fs::read(cache.join("block")).unwrap(), b"cached bytes");
        drop(clone);
        assert!(!cache.exists());
    }

    #[test]
    fn explicit_cache_survives_connections_and_invalid_paths_fail() {
        let directory = tempfile::tempdir().unwrap();
        let requested = directory.path().join("shared '缓存");
        let first = ManagedConnection::in_memory().unwrap();
        let cache = first.cache_directory(Some(&requested)).unwrap();
        assert!(cache.is_absolute());
        std::fs::write(cache.join("block"), b"cached bytes").unwrap();
        drop(first);
        let second = ManagedConnection::in_memory().unwrap();
        assert_eq!(second.cache_directory(Some(&requested)).unwrap(), cache);
        assert_eq!(std::fs::read(cache.join("block")).unwrap(), b"cached bytes");
        assert!(second.cache_directory(Some(&cache.join("block"))).is_err());
        drop(second);
        assert!(cache.join("block").is_file());
    }
}

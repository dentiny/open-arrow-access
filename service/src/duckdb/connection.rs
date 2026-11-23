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
        Ok(Self {
            connection: self.connection.try_clone().map_err(error::database)?,
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

//! Keep scratch storage alive for every DuckDB connection, including import workers.
use super::error;
use duckdb::{Config, Connection};
use open_arrow_access_core::Result;
use std::{ops::Deref, path::Path, sync::Arc};

pub(super) struct MetadataConnection {
    // Fields drop in declaration order: close DuckDB before removing its files.
    connection: Connection,
    directory: Arc<tempfile::TempDir>,
}

impl MetadataConnection {
    fn config() -> Result<(Config, Arc<tempfile::TempDir>)> {
        let directory = Arc::new(tempfile::tempdir().map_err(error::database_io)?);
        let config = Config::default()
            .with("enable_external_file_cache", "false")
            .map_err(error::duckdb)?
            .with(
                "temp_directory",
                directory.path().join("spill").to_string_lossy(),
            )
            .map_err(error::duckdb)?;
        Ok((config, directory))
    }

    pub(super) fn in_memory() -> Result<Self> {
        let (config, directory) = Self::config()?;
        let connection = Connection::open_in_memory_with_flags(config).map_err(error::duckdb)?;
        Ok(Self {
            connection,
            directory,
        })
    }

    pub(super) fn open(path: impl AsRef<Path>, read_only: bool) -> Result<Self> {
        let (mut config, directory) = Self::config()?;
        if read_only {
            config = config
                .with("access_mode", "READ_ONLY")
                .map_err(error::duckdb)?;
        }
        let connection = Connection::open_with_flags(path, config).map_err(error::duckdb)?;
        Ok(Self {
            connection,
            directory,
        })
    }

    pub(super) fn try_clone(&self) -> Result<Self> {
        Ok(Self {
            connection: self.connection.try_clone().map_err(error::duckdb)?,
            directory: self.directory.clone(),
        })
    }

    pub(super) fn close(self) -> Result<()> {
        let result = self
            .connection
            .close()
            .map_err(|(_, source)| error::duckdb(source));
        drop(self.directory);
        result
    }
}

impl Deref for MetadataConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/bhl_metadata_connection.rs"]
mod tests;

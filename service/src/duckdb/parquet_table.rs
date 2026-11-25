use super::{DuckDbDatabase, error};
use open_arrow_access_core::{Error, ErrorKind, Result};

/// Options for external Parquet views. Defaults require compatible file schemas
/// and expose only columns stored in the files, without inferred partition columns.
#[derive(Clone, Copy, Debug, Default)]
pub struct ParquetOptions {
    /// Match columns by name across files, filling unavailable columns with NULL.
    pub union_by_name: bool,
    /// Read `key=value` directory components as additional partition columns.
    pub hive_partitioning: bool,
}

/// An owned description of a Parquet-backed SQL view registered in a DuckDB catalog.
#[derive(Clone, Debug)]
pub struct DuckDBParquetTable {
    pub name: String,
    pub paths: Vec<String>,
    pub options: ParquetOptions,
}

impl DuckDBParquetTable {
    /// Describe a named view over files or supported globs, using default options.
    pub fn new(name: impl Into<String>, paths: &[impl AsRef<str>]) -> Self {
        Self {
            name: name.into(),
            paths: paths.iter().map(|path| path.as_ref().to_owned()).collect(),
            options: ParquetOptions::default(),
        }
    }

    /// Validate the table name and source paths before I/O.
    pub fn validate(&self) -> Result<()> {
        let invalid = |message| Error::new(ErrorKind::InvalidInput, "duckdb_parquet", message);
        if self.name.is_empty() || self.name.contains('\0') {
            return Err(invalid(
                "Parquet table name must be nonempty and contain no NUL",
            ));
        }
        if self.paths.is_empty() {
            return Err(invalid(
                "Parquet registration requires at least one file or glob",
            ));
        }
        for path in &self.paths {
            if path.is_empty() || path.contains('\0') {
                return Err(invalid("Parquet paths must be nonempty and contain no NUL"));
            }
        }
        Ok(())
    }

    /// Register this view without importing rows. Remote files use the database's
    /// cache policy; the view is shared with its cloned and pooled connections.
    pub fn register(&self, database: &DuckDbDatabase) -> Result<()> {
        self.validate()?;
        let connection = database.managed_connection();
        if self
            .paths
            .iter()
            .filter_map(|path| Self::source_url(path))
            .any(|url| url.scheme() != "file")
        {
            connection.configure_remote_cache()?;
        }
        let literals: Vec<_> = self
            .paths
            .iter()
            .map(|path| format!("'{}'", path.replace('\'', "''")))
            .collect();
        // CREATE VIEW does not support bound file parameters. Quote identifiers and
        // string literals separately, keeping caller paths out of SQL syntax.
        let table = self.name.replace('"', "\"\"");
        connection
            .execute_batch(&format!(
                include_str!("create_parquet_view.sql"),
                table = table,
                paths = literals.join(","),
                union_by_name = self.options.union_by_name,
                hive_partitioning = self.options.hive_partitioning
            ))
            .map_err(error::database)
    }

    fn source_url(path: &str) -> Option<url::Url> {
        if path.contains("://") && !std::path::Path::new(path).is_absolute() {
            url::Url::parse(path).ok()
        } else {
            None
        }
    }
}

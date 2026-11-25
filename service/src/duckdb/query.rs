use super::{CachePolicy, ManagedConnection, ParquetOptions, error, parquet};
use ::duckdb::{Params, Statement};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use open_arrow_access_core::Result;
use std::path::Path;

/// Query a DuckDB file or an in-memory catalog of external Parquet tables.
///
/// Existing database files are opened read-only. Opening and querying are synchronous;
/// use a blocking worker in async applications. Remote reads use cache_httpfs:
/// the explicit cache policy controls in-memory or private on-disk caching.
/// No provider-specific schema is required, and query results retain DuckDB's
/// Arrow types and column names.
pub struct DuckDbDatabase {
    // The managed connection retains scratch/cache storage for all clones.
    connection: ManagedConnection,
}

impl DuckDbDatabase {
    /// Create a writable in-memory catalog without a persistent database file.
    /// Optional trusted initialization SQL can load extensions and create secrets;
    /// failures omit its text, as in [`Self::connect`]. Local Parquet registration
    /// requires no extension download. HTTP(S)/S3 registration applies the supplied
    /// cache_httpfs policy. Scratch/spill files may use disk.
    pub fn in_memory(init_sql: Option<&str>, cache_policy: CachePolicy) -> Result<Self> {
        Ok(Self {
            connection: ManagedConnection::in_memory_with_init(init_sql, cache_policy)?,
        })
    }

    /// Register Parquet files or DuckDB-supported globs as one named SQL view.
    /// No rows are copied into DuckDB. Binding reads file metadata; queries read
    /// required columns/pages on demand. Registration uses a shared catalog view,
    /// so clones and pooled connections see it, including clones created earlier.
    /// The name is a single identifier in `main`, and existing objects are not replaced.
    /// Supported remote URLs are HTTP(S)/S3 and always use cache_httpfs;
    /// other remote schemes return an error. Local files need no extension download.
    /// Use [`Self::in_memory`] for a writable catalog; read-only databases reject DDL.
    pub fn register_parquet(&self, table: &str, paths: &[impl AsRef<str>]) -> Result<()> {
        self.register_parquet_with_options(table, paths, ParquetOptions::default())
    }

    /// Register Parquet with explicit schema-union and Hive partition options.
    pub fn register_parquet_with_options(
        &self,
        table: &str,
        paths: &[impl AsRef<str>],
        options: ParquetOptions,
    ) -> Result<()> {
        parquet::register(&self.connection, table, paths, options)
    }

    /// Open an existing file read-only. A missing file is an error, not a newly
    /// created empty database. Paths are passed directly to DuckDB, never SQL.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            connection: ManagedConnection::open(path, true)?,
        })
    }

    /// Open a local path or DuckDB-supported remote URL read-only.
    /// Execute the optional trusted SQL string (e.g. LOAD httpfs; CREATE SECRET ...)
    /// before attaching the database. Secrets are temporary unless the script
    /// explicitly requests persistence. cache_httpfs is force-installed from
    /// community and loaded on each connect; the supplied cache policy is applied
    /// before ATTACH, including local paths.
    /// An on-disk cache uses a caller-supplied directory or an owned temporary
    /// directory retained until the last cloned connection closes.
    /// Only requested blocks are fetched, with no full database download during opening.
    /// The caller can load the SQL from any source or construct it in memory.
    /// SQL execution errors omit the SQL text, which may contain credentials.
    /// This is synchronous; use a blocking worker in async applications.
    pub fn connect(
        location: &str,
        init_sql: Option<&str>,
        cache_policy: CachePolicy,
    ) -> Result<Self> {
        Ok(Self {
            connection: ManagedConnection::connect(location, init_sql, cache_policy)?,
        })
    }

    /// Clone a connection while retaining cached blocks and database-scoped secrets.
    /// Temporary views and tables remain connection-local.
    pub fn try_clone(&self) -> Result<Self> {
        Ok(Self {
            connection: self.connection.try_clone()?,
        })
    }

    pub(crate) fn into_connection(self) -> ManagedConnection {
        self.connection
    }

    /// Execute SQL with bound parameters and emit Arrow batches incrementally.
    ///
    /// Returns the result schema even when no rows match. Returning an error
    /// from `emit` stops fetching immediately. Execution and fetch failures are
    /// returned as errors. Rust does not collect the complete result; DuckDB may
    /// still materialize intermediate results for joins, sorting, or aggregation.
    /// For multiple statements, DuckDB executes preceding statements and returns
    /// the final statement's result. Parameters bind to that final statement.
    pub fn query<P: Params>(
        &self,
        sql: &str,
        params: P,
        emit: impl FnMut(RecordBatch) -> Result<()>,
    ) -> Result<SchemaRef> {
        let mut statement = self.connection.prepare(sql).map_err(error::database)?;
        query_statement(&mut statement, params, emit)
    }

    /// Execute SQL using this connection's prepared-statement cache.
    /// Parameters are rebound on each call; results are streamed as in [`Self::query`].
    pub fn query_cached<P: Params>(
        &self,
        sql: &str,
        params: P,
        emit: impl FnMut(RecordBatch) -> Result<()>,
    ) -> Result<SchemaRef> {
        let mut statement = self
            .connection
            .prepare_cached(sql)
            .map_err(error::database)?;
        query_statement(&mut statement, params, emit)
    }
}

fn query_statement<P: Params>(
    statement: &mut Statement<'_>,
    params: P,
    mut emit: impl FnMut(RecordBatch) -> Result<()>,
) -> Result<SchemaRef> {
    let schema = statement
        .stream_arrow(params)
        .map_err(error::database)?
        .get_schema();
    // The binding's Arrow iterator panics on fetch errors. Use the fallible
    // step API, as the provider-specific metadata queries do.
    while let Some(batch) = statement.step().map_err(error::database)? {
        emit(RecordBatch::from(&batch))?;
    }
    Ok(schema)
}

#[cfg(test)]
#[path = "../../tests/unit/duckdb_query.rs"]
mod tests;

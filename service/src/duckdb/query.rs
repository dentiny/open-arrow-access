use super::{ManagedConnection, error};
use ::duckdb::{Params, Statement};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use open_arrow_access_core::Result;
use std::path::Path;

/// Query a local or remote DuckDB file, including provider metadata databases.
///
/// The database is opened read-only. Opening and querying are synchronous;
/// use a blocking worker in async applications. Remote reads use cache_httpfs
/// with a private temporary on-disk block cache.
/// No provider-specific schema is required, and query results retain DuckDB's
/// Arrow types and column names.
pub struct DuckDbDatabase {
    // The managed connection retains scratch/cache storage for all clones.
    connection: ManagedConnection,
}

impl DuckDbDatabase {
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
    /// community and loaded on each connect; its on-disk cache is always
    /// configured before ATTACH, including local paths.
    /// Only requested
    /// blocks are fetched, with no full database download during opening.
    /// The caller can load the SQL from any source or construct it in memory.
    /// SQL execution errors omit the SQL text, which may contain credentials.
    /// This is synchronous; use a blocking worker in async applications.
    pub fn connect(location: &str, init_sql: Option<&str>) -> Result<Self> {
        Ok(Self {
            connection: ManagedConnection::connect(location, init_sql)?,
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

    /// Reuse a prepared statement on this connection for repeated metadata SQL.
    #[cfg(feature = "arxiv")]
    pub(crate) fn query_cached<P: Params>(
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

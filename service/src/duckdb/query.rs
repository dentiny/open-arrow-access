use super::{ManagedConnection, error};
use ::duckdb::Params;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use open_arrow_access_core::{ByteAccessor, ByteReadRequest, Result};
use std::path::Path;
use tokio::io::AsyncWriteExt;

/// Query a local, attached remote, or staged DuckDB file, including provider metadata databases.
///
/// The database is opened read-only. Local `open` and `query` are synchronous;
/// use a blocking worker in async applications. Remote `open_from` is async.
/// No provider-specific schema is required, and query results retain DuckDB's
/// Arrow types and column names.
pub struct DuckDbDatabase {
    // Close DuckDB before deleting a staged source file.
    connection: ManagedConnection,
    location: String,
}

impl DuckDbDatabase {
    /// Open an existing file read-only. A missing file is an error, not a newly
    /// created empty database. Paths are passed directly to DuckDB, never SQL.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            connection: ManagedConnection::open(&path, true)?,
            location: path.as_ref().to_string_lossy().into_owned(),
        })
    }

    /// Open a local path or DuckDB-supported remote URL read-only.
    /// Execute the optional trusted SQL file (e.g. LOAD httpfs; CREATE SECRET ...)
    /// before attaching the database. Secrets are temporary unless the script
    /// explicitly requests persistence. Required extensions must be available.
    /// This is synchronous; use a blocking worker in async applications.
    pub fn connect(location: &str, secret_sql_file: Option<&Path>) -> Result<Self> {
        Ok(Self {
            connection: ManagedConnection::connect(location, secret_sql_file)?,
            location: location.to_owned(),
        })
    }

    /// Clone a connection while retaining staged files and database-scoped secrets.
    /// Temporary views and tables remain connection-local.
    pub fn try_clone(&self) -> Result<Self> {
        Ok(Self {
            connection: self.connection.try_clone()?,
            location: self.location.clone(),
        })
    }

    #[cfg(feature = "bhl")]
    pub(crate) fn into_connection(self) -> (ManagedConnection, String) {
        (self.connection, self.location)
    }

    /// Download a database through caller-configured storage, then open it read-only.
    ///
    /// Supports S3, HTTP and other locations understood by `storage`. The object
    /// locator is interpreted by the accessor, just like other byte inputs.
    /// Credentials, retries and I/O settings remain with storage and `request`.
    /// The complete file is copied to private temporary disk before opening;
    /// memory use is bounded by the storage reader's chunks. The copy is deleted
    /// on failure or when this database is dropped. Publish a closed/checkpointed
    /// database as one immutable object: separate WAL files are not downloaded.
    pub async fn open_from<A: ByteAccessor + ?Sized>(
        storage: &A,
        request: impl Into<ByteReadRequest>,
    ) -> Result<Self> {
        let request = request.into();
        let location = request.object.locator().to_owned();
        let requested_length = request.content_length;
        let mut reader = storage.open_sequential(request).await?;
        let expected = requested_length.or(reader.metadata().content_length);
        let directory = tempfile::tempdir().map_err(error::scratch)?;
        let path = directory.path().join("source.duckdb");
        let mut file = tokio::fs::File::create(&path)
            .await
            .map_err(error::scratch)?;
        let mut downloaded = 0_u64;
        while let Some(chunk) = reader.next().await? {
            downloaded += chunk.len() as u64;
            if expected.is_some_and(|length| downloaded > length) {
                return Err(error::length_mismatch());
            }
            file.write_all(&chunk).await.map_err(error::scratch)?;
        }
        if expected.is_some_and(|length| downloaded != length) {
            return Err(error::length_mismatch());
        }
        file.flush().await.map_err(error::scratch)?;
        drop(file);
        tokio::task::spawn_blocking(move || {
            let mut connection = ManagedConnection::open(&path, true)?;
            connection.retain_source(directory);
            Ok(Self {
                connection,
                location,
            })
        })
        .await
        .map_err(error::task)?
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
        mut emit: impl FnMut(RecordBatch) -> Result<()>,
    ) -> Result<SchemaRef> {
        let mut statement = self.connection.prepare(sql).map_err(error::database)?;
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
}

#[cfg(test)]
#[path = "../../tests/unit/duckdb_query.rs"]
mod tests;

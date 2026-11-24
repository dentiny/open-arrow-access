//! Budgets for querying an immutable BHL snapshot inside a conversion worker.
use super::error;
use crate::duckdb::ManagedConnection;
use open_arrow_access_core::Result;

pub(super) fn configure(connection: &ManagedConnection) -> Result<()> {
    // The worker shares its memory with Arrow batches, OCR reads and the writer.
    // DuckDB's machine-wide default otherwise retains most of the metadata file
    // in its buffer pool for the entire (potentially slow) OCR stream.
    connection
        .execute_batch(include_str!("resources.sql"))
        .map_err(error::duckdb)?;
    let remote_cache_loaded: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM duckdb_extensions()
             WHERE extension_name = 'cache_httpfs' AND loaded)",
            [],
            |row| row.get(0),
        )
        .map_err(error::duckdb)?;
    if remote_cache_loaded {
        // ATTACH has only populated the loader's disk cache. Configure the
        // bounded memory cache before the first BHL query uses it. Each worker
        // must not retain another multi-GB copy of the remote snapshot on /tmp.
        connection
            .execute_batch(include_str!("remote_cache.sql"))
            .map_err(error::duckdb)?;
    }
    Ok(())
}

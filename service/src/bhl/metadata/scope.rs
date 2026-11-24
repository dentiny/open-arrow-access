//! Bound the correlated metadata joins to the current page selection.
use super::error;
use open_arrow_access_core::Result;

/// Bound metadata join inputs before DuckDB builds wide correlated intermediates.
/// The SQL follows reachable relationships in dependency order while retaining
/// source ordinals, duplicates and conflicting candidates for reconciliation.
/// Its temporary tables belong to this query's private connection.
pub(super) fn prepare(connection: &::duckdb::Connection) -> Result<()> {
    connection
        .execute_batch(include_str!("scope.sql"))
        .map_err(error::duckdb)
}

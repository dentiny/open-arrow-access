//! Query local or remote DuckDB files and manage embedded provider databases.
//!
//! `DuckDbDatabase::connect` executes optional initialization SQL before a read-only
//! local/remote ATTACH. Remote HTTP/S3 reads use cache_httpfs with a private
//! temporary on-disk cache, fetching only requested blocks.
//! BHL uses in-memory or caller-owned persistent databases; CourtListener uses a
//! disposable on-disk database. All modes configure a private spill directory,
//! disable DuckDB's external memory cache, and retain scratch/cache storage
//! with every clone.
//! `DuckDbDatabase` provides a general SQL interface returning Arrow batches
//! without requiring a provider feature.
mod connection;
pub(crate) mod error;

pub(crate) use connection::{ManagedConnection, TableColumn};

mod query;
pub use ::duckdb::{Params, params};
pub use query::DuckDbDatabase;

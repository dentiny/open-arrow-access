//! Query DuckDB databases or registered Parquet files and manage provider databases.
//!
//! `DuckDbDatabase::connect` executes optional initialization SQL before a read-only
//! local/remote ATTACH. Remote HTTP/S3 database reads use cache_httpfs with a private
//! temporary cache selected by the caller. `DuckDbDatabase::in_memory` can register Parquet views;
//! all remote Parquet reads use cache_httpfs with the caller-selected CachePolicy.
//! BHL uses in-memory or caller-owned persistent databases; CourtListener uses a
//! disposable on-disk database. All modes configure a private spill directory,
//! disable DuckDB's external memory cache, and retain scratch/cache storage
//! with every clone.
//! `DuckDbDatabase` provides a general SQL interface returning Arrow batches
//! without requiring a provider feature.
mod cache;
pub use cache::CachePolicy;

mod connection;
mod connection_pool;
pub(crate) mod error;

pub(crate) use connection::ManagedConnection;
#[cfg(feature = "bhl")]
pub(crate) use connection::TableColumn;
pub use connection_pool::{DuckDbConnectionPool, PooledConnection};

mod query;
pub use ::duckdb::{Params, params};
pub use query::DuckDbDatabase;

mod parquet_table;
pub use parquet_table::ParquetOptions;

//! Query local or remote DuckDB files and manage embedded provider databases.
//!
//! `DuckDbDatabase::connect` executes optional secret SQL before a read-only
//! local/remote ATTACH; `open_from` stages caller-configured storage objects.
//! BHL uses in-memory or caller-owned persistent databases; CourtListener uses a
//! disposable on-disk database. All modes configure a private spill directory,
//! disable external file caching, and retain scratch storage with every clone.
//! Enable the `duckdb` service feature (`service-duckdb` in the facade) for
//! `DuckDbDatabase`, a general SQL interface returning Arrow batches.
mod connection;
pub(crate) mod error;

pub(crate) use connection::ManagedConnection;

#[cfg(feature = "duckdb")]
mod query;
#[cfg(feature = "duckdb")]
pub use ::duckdb::{Params, params};
#[cfg(feature = "duckdb")]
pub use query::DuckDbDatabase;

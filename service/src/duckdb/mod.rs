//! Query local or remote DuckDB files and manage embedded provider databases.
//!
//! `DuckDbDatabase::connect` executes optional secret SQL before a read-only
//! local/remote ATTACH; `open_from` stages caller-configured storage objects.
//! BHL uses in-memory or caller-owned persistent databases; CourtListener uses a
//! disposable on-disk database. All modes configure a private spill directory,
//! disable external file caching, and retain scratch storage with every clone.
//! `DuckDbDatabase` provides a general SQL interface returning Arrow batches
//! without requiring a provider feature.
mod connection;
pub(crate) mod error;

pub(crate) use connection::ManagedConnection;

mod query;
pub use ::duckdb::{Params, params};
pub use query::DuckDbDatabase;

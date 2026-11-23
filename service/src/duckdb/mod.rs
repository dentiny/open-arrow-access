//! Shared embedded DuckDB connections and temporary storage for provider services.
//!
//! BHL uses in-memory or caller-owned persistent databases; CourtListener uses a
//! disposable on-disk database. All modes configure a private spill directory,
//! disable external file caching, and retain scratch storage with every clone.
//! Provider schemas, SQL joins, and Arrow conversion stay in their own modules.
mod connection;
pub(crate) mod error;

pub(crate) use connection::ManagedConnection;

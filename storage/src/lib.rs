//! Storage adapters use OpenDAL and expose only the shared read contracts.

#[cfg(feature = "fs")]
mod backend;
#[cfg(feature = "fs")]
mod fs;
#[cfg(feature = "fs")]
pub use fs::FsAccessor;

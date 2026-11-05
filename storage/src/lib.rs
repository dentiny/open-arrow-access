//! Storage adapters use OpenDAL and expose only the shared read contracts.

#[cfg(feature = "operator")]
mod backend;
#[cfg(feature = "operator")]
pub use backend::OpenDalAccessor;
#[cfg(feature = "operator")]
pub use opendal;
#[cfg(feature = "fs")]
mod fs;
#[cfg(feature = "fs")]
pub use fs::FsAccessor;

#[cfg(all(test, feature = "fs"))]
mod tests;

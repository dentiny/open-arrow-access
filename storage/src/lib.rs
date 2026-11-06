//! Storage adapters use OpenDAL and expose only the shared read contracts.

#[cfg(feature = "opendal")]
mod backend;
#[cfg(feature = "fs")]
mod fs;
#[cfg(feature = "fs")]
pub use fs::FsAccessor;

#[cfg(all(test, feature = "fs"))]
mod tests;

#[cfg(feature = "opendal")]
pub use backend::OpenDalAccessor;

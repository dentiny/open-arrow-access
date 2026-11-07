//! Storage adapters use OpenDAL and expose only the shared read contracts.

mod backend;
#[cfg(feature = "fs")]
mod fs;
#[cfg(feature = "fs")]
pub use fs::FsAccessor;

#[cfg(all(test, feature = "fs"))]
mod tests;

pub use backend::OpenDalAccessor;

/// OpenDAL SDK used by the generic adapter.
pub use opendal;

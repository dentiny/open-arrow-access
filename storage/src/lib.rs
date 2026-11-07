//! Storage adapters use OpenDAL and expose only the shared read contracts.

mod backend;
#[cfg(test)]
mod tests;

pub use backend::OpenDalAccessor;

/// OpenDAL SDK used by the generic adapter.
pub use opendal;

//! Storage adapters use OpenDAL and expose only the shared read contracts.

mod backend;
#[cfg(test)]
#[path = "../tests/unit/errors.rs"]
mod error_tests;
mod validation;

pub use backend::OpenDalAccessor;

/// OpenDAL SDK used by the generic adapter.
pub use opendal;

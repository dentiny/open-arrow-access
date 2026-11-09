//! Storage adapters use OpenDAL and expose only the shared read contracts.

mod backend;
mod validation;

pub use backend::OpenDalAccessor;

/// OpenDAL SDK used by the generic adapter.
pub use opendal;

mod named_files;
pub use named_files::{NamedFileAccessor, validate_file_names};

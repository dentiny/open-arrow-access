//! Storage adapters use OpenDAL and expose only the shared read contracts.

mod backend;
mod config;
mod error;
pub use config::{RetryConfig, StorageConfig, TimeoutConfig};
mod validation;

pub use backend::OpenDalAccessor;

/// OpenDAL SDK used by the generic adapter.
pub use opendal;

mod named_files;
pub use named_files::NamedFileAccessor;
pub use validation::validate_file_names;

pub use error::opendal_error_kind;

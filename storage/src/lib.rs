//! Storage adapters use OpenDAL and expose only the shared read contracts.
//!
//! Reuse OpenDAL's concurrent-limit layer for a shared budget across operators:
//! ```no_run
//! use open_arrow_access_storage::{OpenDalAccessor, opendal};
//!
//! fn limited(operator: opendal::Operator, limit: opendal::layers::ConcurrentLimitLayer)
//!     -> OpenDalAccessor
//! {
//!     OpenDalAccessor::new(operator.layer(limit))
//! }
//! let limit = opendal::layers::ConcurrentLimitLayer::new(32)
//!     .with_http_concurrent_limit(8);
//! // Pass limit.clone() to each operator that should share these budgets.
//! ```

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

//! Composable, asynchronous read access to Arrow record batches.
//!
//! Enable `storage-fs` and `format-parquet` for the first supported pipeline.
//! The default build exposes only the shared extension contracts.

pub use open_arrow_access_core::*;

#[cfg(feature = "storage-fs")]
pub mod storage {
    pub use open_arrow_access_storage::FsAccessor;
}

#[cfg(feature = "format-parquet")]
pub mod file_format {
    pub use open_arrow_access_file_format::ParquetAccessor;
}

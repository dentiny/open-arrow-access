//! Composable, asynchronous read access to Arrow record batches.
//!
//! Enable a storage feature and a `format-parquet`, `format-json`, or `format-csv` reader.
//! The default build exposes only the shared extension contracts.

pub use open_arrow_access_core::*;

#[cfg(any(
    feature = "decompression-gzip",
    feature = "decompression-bz2",
    feature = "decompression-lz4",
    feature = "decompression-zstd"
))]
pub mod decompression {
    #[cfg(feature = "decompression-bz2")]
    pub use open_arrow_access_decompression::Bzip2;
    #[cfg(feature = "decompression-gzip")]
    pub use open_arrow_access_decompression::Gzip;
    #[cfg(feature = "decompression-lz4")]
    pub use open_arrow_access_decompression::Lz4;
    #[cfg(feature = "decompression-zstd")]
    pub use open_arrow_access_decompression::Zstd;
    pub use open_arrow_access_decompression::{
        Codec, CompressedInput, DecodedInput, DecompressionAccessor, DecompressionLayer,
    };
}

#[cfg(feature = "storage-fs")]
pub mod storage {
    pub use open_arrow_access_storage::FsAccessor;
}

#[cfg(any(
    feature = "format-parquet",
    feature = "format-json",
    feature = "format-csv"
))]
pub mod file_format {
    #[cfg(feature = "format-json")]
    pub use open_arrow_access_file_format::JsonAccessor;
    #[cfg(feature = "format-parquet")]
    pub use open_arrow_access_file_format::ParquetAccessor;
    #[cfg(feature = "format-csv")]
    pub use open_arrow_access_file_format::{CsvAccessor, CsvFormat};
}

#[cfg(feature = "dataset-directory")]
pub mod dataset {
    pub use open_arrow_access_dataset::{
        DirectoryAccessor, DirectoryRequest, FileSetAccessor, FileSetRequest,
    };
}

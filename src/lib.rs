//! Composable, asynchronous read access to Arrow record batches.
//!
//! The shared extension contracts and OpenDAL storage adapter are always available.
//! Enable a `format-parquet`, `format-json`, `format-csv`, `format-xml`, or
//! `format-atif` reader to decode records.

pub use open_arrow_access_core::*;

#[cfg(any(
    feature = "service-huggingface",
    feature = "service-zenodo",
    feature = "service-dataverse",
    feature = "service-arxiv",
    feature = "service-internet-archive",
    feature = "service-courtlistener",
    feature = "service-pmc",
    feature = "service-warc",
    feature = "service-gutenberg",
    feature = "service-uspto",
    feature = "service-doab",
    feature = "service-hathifiles",
    feature = "service-library-of-congress",
))]
pub use open_arrow_access_service as service;

#[cfg(any(
    feature = "decompression-gzip",
    feature = "decompression-bz2",
    feature = "decompression-lz4",
    feature = "decompression-zstd",
    feature = "decompression-zip",
    feature = "decompression-tar",
))]
pub use open_arrow_access_decompression as decompression;

#[cfg(any(
    feature = "format-warc",
    feature = "format-parquet",
    feature = "format-json",
    feature = "format-atif",
    feature = "format-xml",
    feature = "format-csv",
))]
pub use open_arrow_access_file_format as file_format;

#[cfg(feature = "dataset-directory")]
pub use open_arrow_access_dataset as dataset;

pub use open_arrow_access_storage as storage;

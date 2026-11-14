//! Composable, asynchronous read access to Arrow record batches.
//!
//! The shared extension contracts, OpenDAL storage adapter, service HTTP utilities,
//! file-opening utilities, and record layers (including [`layers::SchemaEvolutionLayer`])
//! are always available. Concrete providers remain opt-in.
//! Enable a `format-parquet`, `format-json`, `format-csv`, `format-xml`, or
//! `format-atif` reader to decode records.

pub use open_arrow_access_core::*;

mod bound_source;
pub use bound_source::BoundSource;

mod file_source;
pub use file_source::{FileSource, OuterCompression, open_file_source};

pub use open_arrow_access_service as service;

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

pub use open_arrow_access_layers as layers;

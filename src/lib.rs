//! Composable, asynchronous read access to Arrow record batches.
//!
//! The shared extension contracts, OpenDAL storage adapter, service HTTP utilities,
//! file-opening utilities and archive expansion are always available.
//! Built-in record layers (`layer-*`), crawling (`crawl`) and concrete providers are opt-in.
//! Enable a `format-parquet`, `format-json`, `format-csv`, `format-xml`, or
//! `format-atif` reader to decode records.

pub use open_arrow_access_core::*;

#[cfg(feature = "blocking")]
mod blocking;
#[cfg(feature = "blocking")]
pub use blocking::BlockingRecordBatchReader;

#[cfg(feature = "crawl")]
pub mod crawl;

mod archive;
pub use archive::{ArchiveAccessor, ArchiveFormat, ArchiveOptions, archive_schema};

mod attachment;
pub use attachment::{
    AttachmentAccessor, AttachmentFormat, AttachmentOptions, open_attachment_source,
};

mod file_source;
pub use file_source::{
    FileSource, OuterCompression, file_accessor, file_scan_accessor, open_file_source,
};

pub use open_arrow_access_service as service;

pub use open_arrow_access_decompression as decompression;

pub use open_arrow_access_file_format as file_format;

pub use open_arrow_access_dataset as dataset;

pub use open_arrow_access_storage as storage;

pub use open_arrow_access_layers as layers;

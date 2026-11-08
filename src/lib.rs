//! Composable, asynchronous read access to Arrow record batches.
//!
//! The shared extension contracts and OpenDAL storage adapter are always available.
//! Enable a `format-parquet`, `format-json`, `format-csv`, `format-xml`, or
//! `format-atif` reader to decode records.

pub use open_arrow_access_core::*;

#[cfg(any(
    feature = "service-huggingface",
    feature = "service-warc",
    feature = "service-gutenberg"
))]
pub mod service {
    #[cfg(feature = "service-gutenberg")]
    pub use open_arrow_access_service::gutenberg;
    #[cfg(feature = "service-gutenberg")]
    pub use open_arrow_access_service::gutenberg::{
        GutenbergAccessor, GutenbergFile, GutenbergLimits, GutenbergRequest,
    };

    #[cfg(feature = "service-warc")]
    pub use open_arrow_access_service::commoncrawl::{
        CommonCrawl, CommonCrawlSegmentAccessor, CommonCrawlSegmentRequest, create_http_accessor,
    };
    #[cfg(feature = "service-huggingface")]
    pub use open_arrow_access_service::huggingface::*;
    #[cfg(feature = "service-warc")]
    pub use open_arrow_access_service::warc::{CommonCrawlWarcAccessor, CommonCrawlWarcRequest};
}

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
        Codec, CompressedInput, DecodedInput, DecompressionAccessor, DecompressionFactory,
        DecompressionLayer,
    };
}

pub mod storage {
    pub use open_arrow_access_storage::OpenDalAccessor;
    pub use open_arrow_access_storage::opendal;
}

#[cfg(any(
    feature = "format-parquet",
    feature = "format-json",
    feature = "format-csv",
    feature = "format-warc",
    feature = "format-xml",
    feature = "format-atif"
))]
pub mod file_format {
    #[cfg(feature = "format-atif")]
    pub use open_arrow_access_file_format::{AtifAccessor, AtifFormat};
    #[cfg(feature = "format-csv")]
    pub use open_arrow_access_file_format::{CsvAccessor, CsvFileFormat, CsvFormat};
    pub use open_arrow_access_file_format::{FileAccessor, FileFormat};
    #[cfg(feature = "format-json")]
    pub use open_arrow_access_file_format::{JsonAccessor, JsonFormat};
    #[cfg(feature = "format-xml")]
    pub use open_arrow_access_file_format::{
        NamespaceBinding, XmlAccessor, XmlField, XmlFormat, XmlLimits, XmlSchema,
    };
    #[cfg(feature = "format-parquet")]
    pub use open_arrow_access_file_format::{ParquetAccessor, ParquetFormat};
    #[cfg(feature = "format-warc")]
    pub use open_arrow_access_file_format::{WarcAccessor, WarcFormat, WarcHeader, WarcRecord};
}

#[cfg(feature = "dataset-directory")]
pub mod dataset {
    pub use open_arrow_access_dataset::{
        DirectoryAccessor, DirectoryRequest, FileSetAccessor, FileSetRequest,
    };
}

//! Streaming decompression through the shared byte accessor and layer contracts.

mod accessor;
mod codec;
mod decoded_reader;
mod factory;
mod layer;

#[cfg(feature = "gzip")]
mod gzip;

pub use accessor::DecompressionAccessor;
pub use codec::{Codec, CompressedInput, DecodedInput};
pub use decoded_reader::decoded_reader;
pub use factory::DecompressionFactory;
pub use layer::DecompressionLayer;

#[cfg(feature = "gzip")]
pub use gzip::Gzip;

#[cfg(feature = "bz2")]
mod bz2;
#[cfg(feature = "bz2")]
pub use bz2::Bzip2;

#[cfg(feature = "lz4")]
mod lz4;
#[cfg(feature = "lz4")]
pub use lz4::Lz4;

#[cfg(feature = "zstd")]
mod zstd;
#[cfg(feature = "zstd")]
pub use zstd::Zstd;

#[cfg(feature = "zip")]
mod zip;
#[cfg(feature = "zip")]
pub use zip::{ZipAccessor, ZipArchive, ZipEntry, ZipLimits};

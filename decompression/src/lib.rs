//! Streaming decompression through the shared byte accessor and layer contracts.

mod accessor;
mod automatic;
mod error;
pub use automatic::{AutoDecompressionAccessor, AutoDecompressionLayer, Compression};
#[cfg(any(feature = "tar", feature = "zip"))]
mod archive;
mod codec;
mod compression_factory;
pub use compression_factory::{CompressionFactory, strip_compression_suffixes};
mod decoded_reader;
mod factory;
mod layer;

pub use accessor::{DecodedAccessor, DecompressionAccessor};
pub use codec::{Codec, CompressedInput, DecodedInput};
pub use decoded_reader::decoded_reader;
pub use factory::DecompressionFactory;
pub use layer::DecompressionLayer;

#[cfg(feature = "gzip")]
pub mod gzip;

#[cfg(feature = "bz2")]
pub mod bz2;

#[cfg(feature = "lz4")]
pub mod lz4;

#[cfg(feature = "zstd")]
pub mod zstd;

#[cfg(feature = "zip")]
pub mod zip;

#[cfg(feature = "tar")]
pub mod tar;

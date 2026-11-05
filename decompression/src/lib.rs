//! Streaming decompression through the shared byte accessor and layer contracts.

mod accessor;
mod codec;
mod layer;
mod reader;

#[cfg(feature = "gzip")]
mod gzip;
#[cfg(feature = "zstd")]
mod zstd;

pub use accessor::DecompressionAccessor;
pub use codec::{Codec, CompressedInput, DecodedInput};
pub use layer::DecompressionLayer;

#[cfg(feature = "gzip")]
pub use gzip::Gzip;
#[cfg(feature = "zstd")]
pub use zstd::Zstd;

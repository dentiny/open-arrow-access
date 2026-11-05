//! Streaming decompression through the shared byte accessor and layer contracts.

mod accessor;
mod codec;
mod layer;
mod reader;

#[cfg(feature = "gzip")]
mod gzip;

pub use accessor::DecompressionAccessor;
pub use codec::{Codec, CompressedInput, DecodedInput};
pub use layer::DecompressionLayer;

#[cfg(feature = "gzip")]
pub use gzip::Gzip;

#[cfg(feature = "lz4")]
mod lz4;
#[cfg(feature = "lz4")]
pub use lz4::Lz4;

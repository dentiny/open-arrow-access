use std::pin::Pin;

use futures::io::{AsyncBufRead, AsyncRead};

/// Owned compressed input retaining the upstream byte reader and its errors.
pub type CompressedInput = Pin<Box<dyn AsyncBufRead + Send>>;
/// Owned sequential decoder. Decoding starts only when the reader is polled.
pub type DecodedInput = Pin<Box<dyn AsyncRead + Send>>;

/// A streaming decoder constructor, independent of storage and Arrow formats.
pub trait Codec: Clone + Send + Sync + 'static {
    /// Return the codec name used in decoding error context, without I/O.
    fn name(&self) -> &'static str;

    /// Wrap input without consuming it or starting background tasks.
    ///
    /// Decoders must report truncated/corrupt streams, preserve upstream I/O
    /// errors in their source chain, and validate all compressed members through
    /// EOF. The returned stream uses decoded byte coordinates.
    fn decoder(&self, input: CompressedInput) -> DecodedInput;
}

// Each streaming codec uses the same member and terminal-validation policy.
#[cfg(any(feature = "gzip", feature = "bz2", feature = "lz4", feature = "zstd"))]
macro_rules! async_compression_codec {
    ($(#[$doc:meta])* $ty:ident, $name:literal, $($decoder:ident)::+) => {
        $(#[$doc])*
        #[derive(Clone, Copy, Debug, Default)]
        pub struct $ty;

        impl $crate::Codec for $ty {
            fn name(&self) -> &'static str {
                $name
            }

            fn decoder(&self, input: $crate::CompressedInput) -> $crate::DecodedInput {
                let mut decoder = $($decoder)::+::new(input);
                decoder.multiple_members(true);
                Box::pin(decoder)
            }
        }
    };
}

#[cfg(any(feature = "gzip", feature = "bz2", feature = "lz4", feature = "zstd"))]
pub(crate) use async_compression_codec;

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

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
    /// Decode exactly one frame, or reject position tracking for custom codecs.
    fn frame_decoder(
        &self,
        _input: CompressedInput,
    ) -> open_arrow_access_core::Result<DecodedFrame> {
        Err(open_arrow_access_core::Error::new(
            open_arrow_access_core::ErrorKind::Unsupported,
            "frame_decoder",
            "codec does not support single-frame decoding",
        ))
    }
}

/// A single-frame decoder that returns the buffered compressed input at EOF.
/// No bytes belonging to the following frame may be consumed.
pub trait FrameDecoder: AsyncRead + Unpin + Send {
    fn into_input(self: Box<Self>) -> CompressedInput;
}
pub type DecodedFrame = Box<dyn FrameDecoder>;

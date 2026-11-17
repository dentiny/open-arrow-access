use async_compression::futures::bufread::Lz4Decoder;

use crate::{Codec, CompressedInput, DecodedInput};

/// Standard LZ4 frames, including concatenated frames and skippable frames.
/// Raw blocks and the legacy LZ4 frame format are not supported.
#[derive(Clone, Copy, Debug, Default)]
pub struct Lz4;

impl Codec for Lz4 {
    fn name(&self) -> &'static str {
        "lz4"
    }

    fn decoder(&self, input: CompressedInput) -> DecodedInput {
        let mut decoder = Lz4Decoder::new(input);
        decoder.multiple_members(true);
        Box::pin(decoder)
    }
}

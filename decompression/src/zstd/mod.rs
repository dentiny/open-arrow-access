use async_compression::futures::bufread::ZstdDecoder;

use crate::{Codec, CompressedInput, DecodedInput};

/// Standard Zstandard frames, including concatenated and skippable frames.
#[derive(Clone, Copy, Debug, Default)]
pub struct Zstd;

impl Codec for Zstd {
    fn name(&self) -> &'static str {
        "zstd"
    }

    fn decoder(&self, input: CompressedInput) -> DecodedInput {
        let mut decoder = ZstdDecoder::new(input);
        decoder.multiple_members(true);
        Box::pin(decoder)
    }
}

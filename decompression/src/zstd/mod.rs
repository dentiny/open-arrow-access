use async_compression::futures::bufread::ZstdDecoder;

use crate::{Codec, CompressedInput, DecodedInput};

/// Standard Zstandard frames, including concatenated and skippable frames.
#[derive(Clone, Copy, Debug, Default)]
pub struct Zstd;

impl Codec for Zstd {
    fn name(&self) -> &'static str {
        "zstd"
    }

    fn frame_decoder(
        &self,
        input: CompressedInput,
    ) -> open_arrow_access_core::Result<crate::DecodedFrame> {
        let mut decoder = ZstdDecoder::new(input);
        decoder.multiple_members(false);
        Ok(Box::new(decoder))
    }

    fn decoder(&self, input: CompressedInput) -> DecodedInput {
        let mut decoder = ZstdDecoder::new(input);
        decoder.multiple_members(true);
        Box::pin(decoder)
    }
}

impl crate::FrameDecoder for ZstdDecoder<CompressedInput> {
    fn into_input(self: Box<Self>) -> CompressedInput {
        (*self).into_inner()
    }
}

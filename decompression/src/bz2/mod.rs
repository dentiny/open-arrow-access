use async_compression::futures::bufread::BzDecoder;

use crate::{Codec, CompressedInput, DecodedInput};

/// Bzip2 decoding with concatenated-member support and strict trailing data checks.
#[derive(Clone, Copy, Debug, Default)]
pub struct Bzip2;

impl Codec for Bzip2 {
    fn name(&self) -> &'static str {
        "bzip2"
    }

    fn frame_decoder(
        &self,
        input: CompressedInput,
    ) -> open_arrow_access_core::Result<crate::DecodedFrame> {
        let mut decoder = BzDecoder::new(input);
        decoder.multiple_members(false);
        Ok(Box::new(decoder))
    }

    fn decoder(&self, input: CompressedInput) -> DecodedInput {
        let mut decoder = BzDecoder::new(input);
        decoder.multiple_members(true);
        Box::pin(decoder)
    }
}

impl crate::FrameDecoder for BzDecoder<CompressedInput> {
    fn into_input(self: Box<Self>) -> CompressedInput {
        (*self).into_inner()
    }
}

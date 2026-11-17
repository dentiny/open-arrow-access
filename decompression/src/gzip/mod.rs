use async_compression::futures::bufread::GzipDecoder;

use crate::{Codec, CompressedInput, DecodedInput};

/// Gzip decoding with concatenated-member support and strict trailing data checks.
#[derive(Clone, Copy, Debug, Default)]
pub struct Gzip;

impl Codec for Gzip {
    fn name(&self) -> &'static str {
        "gzip"
    }

    fn frame_decoder(
        &self,
        input: CompressedInput,
    ) -> open_arrow_access_core::Result<crate::DecodedFrame> {
        let mut decoder = GzipDecoder::new(input);
        decoder.multiple_members(false);
        Ok(Box::new(decoder))
    }

    fn decoder(&self, input: CompressedInput) -> DecodedInput {
        let mut decoder = GzipDecoder::new(input);
        decoder.multiple_members(true);
        Box::pin(decoder)
    }
}

impl crate::FrameDecoder for GzipDecoder<CompressedInput> {
    fn into_input(self: Box<Self>) -> CompressedInput {
        (*self).into_inner()
    }
}

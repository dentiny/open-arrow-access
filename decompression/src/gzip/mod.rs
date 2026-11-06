use async_compression::futures::bufread::GzipDecoder;

use crate::{Codec, CompressedInput, DecodedInput};

/// Gzip decoding with concatenated-member support and strict trailing data checks.
#[derive(Clone, Copy, Debug, Default)]
pub struct Gzip;

impl Codec for Gzip {
    fn name(&self) -> &'static str {
        "gzip"
    }

    fn decoder(&self, input: CompressedInput) -> DecodedInput {
        let mut decoder = GzipDecoder::new(input);
        decoder.multiple_members(true);
        Box::pin(decoder)
    }
}

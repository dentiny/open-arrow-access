use async_compression::futures::bufread::BzDecoder;

use crate::{Codec, CompressedInput, DecodedInput};

/// Bzip2 decoding with concatenated-member support and strict trailing data checks.
#[derive(Clone, Copy, Debug, Default)]
pub struct Bzip2;

impl Codec for Bzip2 {
    fn name(&self) -> &'static str {
        "bzip2"
    }

    fn decoder(&self, input: CompressedInput) -> DecodedInput {
        let mut decoder = BzDecoder::new(input);
        decoder.multiple_members(true);
        Box::pin(decoder)
    }
}

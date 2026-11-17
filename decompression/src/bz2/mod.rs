crate::codec::async_compression_codec! {
    /// Bzip2 decoding with concatenated-member support and strict trailing data checks.
    Bzip2, "bzip2", async_compression::futures::bufread::BzDecoder
}

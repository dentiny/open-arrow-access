crate::codec::async_compression_codec! {
    /// Standard Zstandard frames, including concatenated and skippable frames.
    Zstd, "zstd", async_compression::futures::bufread::ZstdDecoder
}

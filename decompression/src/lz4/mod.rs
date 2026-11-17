crate::codec::async_compression_codec! {
    /// Standard LZ4 frames, including concatenated frames and skippable frames.
    /// Raw blocks and the legacy LZ4 frame format are not supported.
    Lz4, "lz4", async_compression::futures::bufread::Lz4Decoder
}

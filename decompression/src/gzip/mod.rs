crate::codec::async_compression_codec! {
    /// Gzip decoding with concatenated-member support and strict trailing data checks.
    Gzip, "gzip", async_compression::futures::bufread::GzipDecoder
}

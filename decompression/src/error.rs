use open_arrow_access_core::{Error, ErrorKind};

#[cfg(any(
    not(feature = "gzip"),
    not(feature = "bz2"),
    not(feature = "lz4"),
    not(feature = "zstd")
))]
pub(crate) fn disabled_codec(feature: &str) -> Error {
    Error::new(
        ErrorKind::Unsupported,
        "auto_decompress",
        format!("compression requires enabling the `{feature}` codec feature"),
    )
}

pub(crate) fn decoder_error(
    error: std::io::Error,
    operation: &'static str,
    codec_name: &str,
) -> Error {
    Error::new(
        ErrorKind::from_source(&error).unwrap_or(ErrorKind::Decode),
        operation,
        format!("{codec_name} decoding failed"),
    )
    .with_source(error)
}

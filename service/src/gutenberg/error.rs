use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn decode(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Decode, "read_gutenberg", message)
}

pub(super) fn decode_source(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    decode(error.to_string()).with_source(error)
}

pub(super) fn invalid_mirror_uri(uri: &str) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "gutenberg_mirror",
        format!("unsupported or unsafe Gutenberg file URI: {uri}"),
    )
}

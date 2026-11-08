use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn decode(message: &str) -> Error {
    Error::new(ErrorKind::Decode, "read_tar", message)
}

pub(super) fn unsupported(message: &str) -> Error {
    Error::new(ErrorKind::Unsupported, "read_tar", message)
}

pub(super) fn header_error(error: std::io::Error) -> Error {
    decode("invalid TAR header").with_source(error)
}

pub(super) fn missing_entry() -> Error {
    Error::new(
        ErrorKind::NotFound,
        "open_tar_entry",
        "TAR entry index is out of bounds",
    )
}

pub(super) fn active_entry() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "read_tar",
        "consume or drop the current entry reader before advancing",
    )
}

pub(super) fn empty_chunk() -> Error {
    Error::new(
        ErrorKind::Io,
        "read_tar",
        "byte reader returned an empty chunk",
    )
}

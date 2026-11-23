use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn decode(message: &str) -> Error {
    Error::new(ErrorKind::Decode, "read_tar", message)
}

pub(super) fn unsupported(message: &str) -> Error {
    Error::new(ErrorKind::Unsupported, "read_tar", message)
}

pub(super) fn header_error(error: std::io::Error) -> Error {
    let kind = ErrorKind::from_source(&error).unwrap_or(ErrorKind::Decode);
    Error::new(kind, "read_tar", "TAR package read failed").with_source(error)
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

pub(super) fn runtime(error: tokio::runtime::TryCurrentError) -> Error {
    unsupported("TAR package adapter requires a Tokio runtime").with_source(error)
}

pub(super) fn worker(error: tokio::task::JoinError) -> Error {
    Error::new(ErrorKind::Io, "read_tar", "TAR package worker failed").with_source(error)
}

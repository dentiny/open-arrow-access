use open_arrow_access_core::{Error, ErrorKind};

pub(crate) fn injected_open(kind: ErrorKind) -> Error {
    Error::new(kind, "memory_open", "injected open failure").with_source(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "injected open failure",
    ))
}

pub(crate) fn injected_read(kind: ErrorKind) -> Error {
    Error::new(kind, "memory_read", "injected source failure").with_source(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "injected source failure",
    ))
}

pub(crate) fn injected_range(kind: ErrorKind) -> Error {
    Error::new(kind, "memory_range", "injected range failure").with_source(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "injected range failure",
    ))
}

pub(crate) fn listing_disabled() -> Error {
    Error::new(ErrorKind::Unsupported, "list", "listing is disabled")
}

pub(crate) fn sequential_only() -> Error {
    Error::new(ErrorKind::Unsupported, "read_range", "sequential only")
}

pub(crate) fn invalid_range() -> Error {
    Error::new(ErrorKind::InvalidInput, "read_range", "invalid range")
}

use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn decode(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Decode, "read_pmc", message)
}

pub(super) fn invalid_metadata_path() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "list_pmc_metadata",
        "expected an exact .json metadata object or a directory ending in '/'",
    )
}

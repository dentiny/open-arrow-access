use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn decode(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Decode, "read_pmc_inventory", message)
}

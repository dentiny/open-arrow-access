//! Read one PMC inventory snapshot without fetching articles.
mod accessor;
mod manifest;
mod reader;
mod validation;

pub use accessor::PmcInventoryAccessor;

use open_arrow_access_core::{Capability, Error, ErrorKind};
fn capability(read: bool) -> Capability {
    Capability {
        read,
        ..Capability::default()
    }
}
fn decode(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Decode, "read_pmc_inventory", message)
}

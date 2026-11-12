use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn decode(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Decode, "read_pmc_inventory", message)
}

pub(super) fn unsupported_inventory_format() -> Error {
    Error::new(
        ErrorKind::Unsupported,
        "open_pmc_inventory",
        "only CSV inventory manifests are supported",
    )
}

pub(super) fn manifest_json(source: serde_json::Error) -> Error {
    decode(source.to_string()).with_source(source)
}

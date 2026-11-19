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

pub(super) fn unknown_shard(shard: &open_arrow_access_core::ObjectRef) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "select_pmc_inventory_shard",
        "shard is not listed in the inventory manifest",
    )
    .with_object(Some(shard))
}
pub(super) fn invalid_shard() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "select_pmc_inventory_shard",
        "expected an exact .csv.gz inventory shard key",
    )
}
pub(super) fn invalid_metadata_key() -> Error {
    decode(
        "inventory Key must identify a JSON object below metadata/ without empty or dot path components",
    )
}
pub(super) fn key_encoding(source: std::str::Utf8Error) -> Error {
    decode("inventory Key is not valid UTF-8").with_source(source)
}

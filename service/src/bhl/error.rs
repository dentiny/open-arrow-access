use crate::common::decode;
use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn unauthorized() -> open_arrow_access_core::Error {
    Error::new(
        ErrorKind::PermissionDenied,
        "resolve_bhl",
        "BHL API key is unauthorized",
    )
}

pub(super) fn api_error() -> Error {
    decode("BHL returned an API error")
}

pub(super) fn item_identity_mismatch() -> Error {
    decode("BHL item identity mismatch")
}

pub(super) fn configure_open_data(source: opendal::Error) -> Error {
    invalid_open_data("failed to configure public BHL bucket").with_source(source)
}

pub(super) fn empty_open_data_partition() -> Error {
    invalid_open_data("empty BHL Open Data partition")
}

pub(super) fn invalid_open_data_key() -> Error {
    invalid_open_data("expected a bucket-relative BHL Open Data key")
}

pub(super) fn invalid_open_data_component() -> Error {
    invalid_open_data("BHL Open Data key has an invalid path component")
}

pub(super) fn open_data_object_required() -> Error {
    invalid_open_data("expected a BHL Open Data object, not a directory")
}

fn invalid_open_data(message: &str) -> Error {
    Error::new(ErrorKind::InvalidInput, "bhl_open_data", message)
}

pub(super) fn page_record(source: arrow_schema::ArrowError) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "bhl_page_record",
        source.to_string(),
    )
    .with_source(source)
}

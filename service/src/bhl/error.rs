use open_arrow_access_core::{Error, ErrorKind, Result};

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

pub(super) fn invalid_pages(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidInput, "bhl_pages", message)
}

pub(super) fn require_page_metadata(page: &super::metadata::BhlPageMetadata) -> Result<()> {
    if page.missing.is_empty() && page.conflicts.is_empty() {
        return Ok(());
    }
    Err(invalid_pages(format!(
        "missing metadata {:?}; conflicting metadata {:?}",
        page.missing, page.conflicts
    ))
    .with_object(Some(&page.object)))
}

pub(super) fn metadata_worker(source: tokio::task::JoinError) -> Error {
    Error::new(ErrorKind::Internal, "bhl_metadata", source.to_string()).with_source(source)
}

use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn map_error(operation: &'static str, path: &str, error: opendal::Error) -> Error {
    let kind = crate::error::error_kind(error.kind());
    Error::new(kind, operation, format!("Hugging Face object {path:?}")).with_source(error)
}

pub(super) fn invalid_repository() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "configure_huggingface",
        "expected a repository ID in owner/name form",
    )
}

pub(super) fn invalid_revision() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "configure_huggingface",
        "invalid repository revision",
    )
}

pub(super) fn invalid_token() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "configure_huggingface",
        "invalid Hub authentication token",
    )
}

pub(super) fn invalid_endpoint() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "configure_huggingface",
        "expected an HTTP(S) Hub endpoint without credentials, query or fragment",
    )
}

pub(super) fn partial_export_requires_opt_in() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "open_huggingface_dataset",
        "partial exports require explicit opt-in",
    )
}

pub(super) fn missing_commit_sha() -> Error {
    Error::new(
        ErrorKind::Decode,
        "resolve_huggingface",
        "revision metadata did not contain a commit SHA",
    )
}

pub(super) fn endpoint_path() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "configure_huggingface",
        "invalid Hub endpoint",
    )
}

pub(super) fn empty_split() -> Error {
    Error::new(
        ErrorKind::NotFound,
        "resolve_huggingface",
        "selected split contains no Parquet shards",
    )
}

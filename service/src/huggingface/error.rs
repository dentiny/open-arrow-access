use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn map_error(operation: &'static str, path: &str, error: opendal::Error) -> Error {
    let kind = crate::error::error_kind(&error);
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

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::*;

    #[test]
    fn mapped_errors_preserve_nested_core_categories_and_context() {
        for kind in [
            ErrorKind::Timeout,
            ErrorKind::RateLimited,
            ErrorKind::Decode,
        ] {
            let source = Error::new(kind, "provider_http", "upstream failure");
            let error = opendal::Error::new(opendal::ErrorKind::Unexpected, "SDK wrapper")
                .set_source(source);
            let mapped = map_error("open_huggingface", "dataset/data.parquet", error);
            assert_eq!(mapped.kind(), kind);
            assert_eq!(mapped.operation(), "open_huggingface");
            assert!(mapped.to_string().contains("dataset/data.parquet"));
            assert!(mapped.source().unwrap().is::<opendal::Error>());
        }
    }

    #[test]
    fn configuration_fallback_does_not_override_nested_category() {
        let plain = opendal::Error::new(opendal::ErrorKind::ConfigInvalid, "bad configuration");
        assert_eq!(
            map_error("configure_huggingface", "repo", plain).kind(),
            ErrorKind::InvalidInput
        );
        let nested = opendal::Error::new(opendal::ErrorKind::ConfigInvalid, "SDK wrapper")
            .set_source(Error::new(ErrorKind::Timeout, "provider_http", "timeout"));
        assert_eq!(
            map_error("configure_huggingface", "repo", nested).kind(),
            ErrorKind::Timeout
        );
    }
}

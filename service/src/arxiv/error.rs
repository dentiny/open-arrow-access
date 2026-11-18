use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn invalid(message: &str) -> Error {
    Error::new(ErrorKind::InvalidInput, "configure_arxiv", message)
}

pub(super) fn decode(message: &str) -> Error {
    Error::new(ErrorKind::Decode, "resolve_arxiv", message)
}

pub(super) fn xml_error(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    decode("invalid arXiv bulk manifest XML").with_source(error)
}

pub(super) fn arrow_error(error: arrow_schema::ArrowError) -> Error {
    decode("invalid arXiv metadata batch").with_source(error)
}

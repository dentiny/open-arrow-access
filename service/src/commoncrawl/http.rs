use open_arrow_access_core::{Error, ErrorKind, Result};
use open_arrow_access_storage::OpenDalAccessor;
use url::Url;

/// Configure OpenDAL HTTP access rooted at an origin, without reading objects.
///
/// Use the request's `origin()` for mirrors. The supplied URL must contain only
/// the HTTP(S) origin, with an optional root slash. HTTP does not expose listing;
/// the manifest provider discovers its file set instead.
pub fn create_http_accessor(origin: &str) -> Result<OpenDalAccessor> {
    let url = Url::parse(origin).map_err(|e| invalid_origin().with_source(e))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
        || origin.trim() != origin
        || origin.chars().any(char::is_control)
        || origin.contains(['%', '\\'])
        || origin.split('/').any(|part| matches!(part, "." | ".."))
    {
        return Err(invalid_origin());
    }
    opendal::install_default();
    let operator = opendal::Operator::new(
        opendal_service_http::Http::default().endpoint(&url.origin().ascii_serialization()),
    )
    .map_err(|e| {
        Error::new(
            ErrorKind::InvalidInput,
            "configure_commoncrawl_http",
            "invalid HTTP configuration",
        )
        .with_source(e)
    })?;
    Ok(OpenDalAccessor::new(operator))
}

fn invalid_origin() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "configure_commoncrawl_http",
        "expected an HTTP(S) origin without credentials, path, query or fragment",
    )
}

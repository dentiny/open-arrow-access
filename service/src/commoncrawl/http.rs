use open_arrow_access_core::{Error, ErrorKind, Result};
use open_arrow_access_storage::OpenDalAccessor;

use super::url::parse_url;

/// Configure an origin-rooted HTTP accessor without performing I/O.
/// Gzip and format decoders are supplied separately by the caller.
pub fn create_http_accessor(origin: &str) -> Result<OpenDalAccessor> {
    let url = parse_url(origin)?;
    if url.path() != "/" {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "configure_commoncrawl_http",
            "expected an HTTP(S) origin without a path",
        ));
    }
    opendal::install_default();
    let operator = opendal::Operator::new(
        opendal_service_http::Http::default().endpoint(&url.origin().ascii_serialization()),
    )
    .map_err(|error| {
        Error::new(
            ErrorKind::InvalidInput,
            "configure_commoncrawl_http",
            "invalid HTTP configuration",
        )
        .with_source(error)
    })?;
    Ok(OpenDalAccessor::new(operator))
}

use super::error;
use open_arrow_access_core::Result;

pub(crate) const MAX_REDIRECTS: usize = 10;

/// Build the default HTTP transport with automatic redirects enabled.
pub(crate) fn new_transport() -> Result<impl opendal::HttpTransport> {
    build(reqwest::redirect::Policy::limited(MAX_REDIRECTS))
}

/// Send one hop beneath the provider redirect/authentication transport.
pub(crate) fn new_single_hop_transport() -> Result<impl opendal::HttpTransport> {
    build(reqwest::redirect::Policy::none())
}

fn build(redirect: reqwest::redirect::Policy) -> Result<impl opendal::HttpTransport> {
    let client = reqwest::Client::builder()
        .redirect(redirect)
        .user_agent(concat!("open-arrow-access/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(error::client_error)?;
    Ok(opendal_http_transport_reqwest::ReqwestTransport::new(
        client,
    ))
}

use super::error;
use open_arrow_access_core::Result;

/// Build an isolated transport; callers decide whether to install it as a default.
pub(crate) fn new_transport(follow_redirects: bool) -> Result<impl opendal::HttpTransport> {
    let redirect = if follow_redirects {
        reqwest::redirect::Policy::limited(10)
    } else {
        reqwest::redirect::Policy::none()
    };
    let client = reqwest::Client::builder()
        .redirect(redirect)
        .user_agent(concat!("open-arrow-access/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(error::client_error)?;
    Ok(opendal_http_transport_reqwest::ReqwestTransport::new(
        client,
    ))
}

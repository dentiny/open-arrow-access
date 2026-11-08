use http::Request;
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter};
use url::Url;

use super::error::invalid_download_url;

/// Fixed public query parameters shared by metadata and range download routes.
#[derive(Debug)]
pub(super) struct QueryTransport {
    pub(super) inner: HttpTransporter,
    pub(super) query: Vec<(String, String)>,
}

impl HttpTransport for QueryTransport {
    async fn fetch(
        &self,
        mut request: Request<Buffer>,
    ) -> opendal::Result<http::Response<HttpBody>> {
        let mut url = Url::parse(&request.uri().to_string()).map_err(invalid_download_url)?;
        url.query_pairs_mut()
            .extend_pairs(self.query.iter().map(|(key, value)| (key, value)));
        *request.uri_mut() = url.as_str().parse().map_err(invalid_download_url)?;
        self.inner.fetch(request).await
    }
}

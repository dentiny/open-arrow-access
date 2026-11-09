//! Default redirect handling for provider GET requests and origin-scoped credentials.
use super::{error, resolve_download_url, url_util};
use http::{HeaderMap, Request};
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter, OperationContext};
use url::Url;

#[derive(Clone)]
pub(super) struct AuthScope {
    pub(super) origin: Url,
    pub(super) headers: HeaderMap,
    pub(super) query: Vec<(String, String)>,
}

/// The inner transport sends one hop; this layer follows redirects by default.
pub(super) fn follow_redirects(context: OperationContext) -> OperationContext {
    let inner = context.http_transport().clone();
    context.with_http_transport(HttpTransporter::new(RedirectTransport { inner }))
}

#[derive(Debug)]
struct RedirectTransport {
    inner: HttpTransporter,
}
impl HttpTransport for RedirectTransport {
    async fn fetch(
        &self,
        mut request: Request<Buffer>,
    ) -> opendal::Result<http::Response<HttpBody>> {
        let scope = request
            .extensions_mut()
            .remove::<AuthScope>()
            .ok_or_else(error::missing_auth_scope)?;
        let mut url = resolve_download_url(&request.uri().to_string()).map_err(error::transport)?;
        let mut authorized = url.origin() == scope.origin.origin();
        for _ in 0..=crate::http_client::MAX_REDIRECTS {
            if authorized {
                for (name, value) in &scope.query {
                    if !url.query_pairs().any(|(key, _)| key == name.as_str()) {
                        url.query_pairs_mut().append_pair(name, value);
                    }
                }
            }
            let mut next = Request::builder()
                .method(request.method().clone())
                .version(request.version())
                .uri(url.as_str())
                .body(request.body().clone())
                .map_err(error::transport_request)?;
            next.headers_mut().extend(request.headers().clone());
            if authorized {
                next.headers_mut().extend(scope.headers.clone());
            }
            let response = self.inner.fetch(next).await?;
            if matches!(response.status().as_u16(), 301 | 302 | 303 | 307 | 308) {
                let next =
                    url_util::redirect(&url, response.headers()).map_err(error::transport)?;
                authorized = authorized && next.origin() == url.origin();
                url = next;
                continue;
            }
            return Ok(response);
        }
        Err(error::transport(error::decode("too many redirects")))
    }
}

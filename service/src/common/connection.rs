use super::{
    ApiDocument, download_url, error, url_util,
    validation::{self, BodyLength},
};
use http::{HeaderMap, HeaderValue, Request, header::HeaderName};
use open_arrow_access_core::{Bytes, Result};
use opendal::{Buffer, HttpBody, HttpTransporter, OperationContext, raw::oio::ReadStream};
use url::Url;

/// HTTP settings. Debug output never includes keys or signed URLs.
#[derive(Clone)]
pub struct Connection {
    pub(super) origin: Url,
    pub(super) headers: HeaderMap,
    pub(super) query: Vec<(String, String)>,
    pub(super) context: OperationContext,
}
impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("origin", &self.origin)
            .finish_non_exhaustive()
    }
}
impl Connection {
    pub fn new(origin: &str) -> Result<Self> {
        let origin = crate::http_client::validation::validate_origin(origin, error::OP)?;
        // Handle redirects here so arbitrary API-key headers cannot escape their origin.
        let context = OperationContext::new().with_http_transport(HttpTransporter::new(
            crate::http_client::new_transport(false)?,
        ));
        Ok(Self {
            origin,
            headers: HeaderMap::new(),
            query: vec![],
            context,
        })
    }
    /// Override transport, for custom TLS/proxy configuration or deterministic tests.
    /// Its redirect policy must leave redirects visible to this connection.
    pub fn with_context(mut self, context: OperationContext) -> Self {
        self.context = context;
        self
    }
    pub fn origin(&self) -> &Url {
        &self.origin
    }
    pub fn with_header(mut self, name: &str, value: &str) -> Result<Self> {
        let name = HeaderName::from_bytes(name.as_bytes()).map_err(error::header_name)?;
        let mut value = HeaderValue::from_str(value).map_err(error::header_value)?;
        value.set_sensitive(true);
        self.headers.insert(name, value);
        Ok(self)
    }
    /// Scoped query authentication, e.g. GovInfo's `api_key`.
    pub fn with_query(mut self, name: &str, value: &str) -> Self {
        self.query.push((name.into(), value.into()));
        self
    }
    pub fn url(&self, path: &str, query: &[(&str, &str)]) -> Result<Url> {
        url_util::provider_url(&self.origin, path, query)
    }

    pub(super) fn request(
        &self,
        url: &mut Url,
        accept: &str,
        authorized: bool,
    ) -> Result<Request<Buffer>> {
        if authorized {
            for (name, value) in &self.query {
                if !url.query_pairs().any(|(key, _)| key == name.as_str()) {
                    url.query_pairs_mut().append_pair(name, value);
                }
            }
        }
        let mut request = Request::get(url.as_str())
            .header("accept", accept)
            .body(Buffer::new())
            .map_err(error::request)?;
        if authorized {
            request.headers_mut().extend(self.headers.clone());
        }
        Ok(request)
    }

    pub(super) async fn response(
        &self,
        url: Url,
        accept: &str,
    ) -> Result<http::Response<HttpBody>> {
        let mut url = download_url(url.as_str())?;
        let mut authorized = url.origin() == self.origin.origin();
        for _ in 0..=10 {
            let request = self.request(&mut url, accept, authorized)?;
            let response = self
                .context
                .http_transport()
                .fetch(request)
                .await
                .map_err(error::io)?;
            let status = response.status();
            if matches!(status.as_u16(), 301 | 302 | 303 | 307 | 308) {
                let next = url_util::redirect(&url, response.headers())?;
                authorized = authorized && next.origin() == url.origin();
                url = next;
                continue;
            }
            if status != http::StatusCode::OK {
                return Err(error::http(status));
            }
            return Ok(response);
        }
        Err(error::decode("too many redirects"))
    }
    /// Fetch one API page as parsed JSON metadata.
    pub async fn json(&self, path: &str, query: &[(&str, &str)]) -> Result<ApiDocument> {
        self.json_url(self.url(path, query)?).await
    }
    /// Fetch an API continuation URL only if it remains within the configured origin.
    pub async fn json_url(&self, url: Url) -> Result<ApiDocument> {
        validation::same_origin(&self.origin, &url)?;
        let bytes = self.bytes(url, "application/json").await?;
        ApiDocument::from_bytes(bytes)
    }
    /// Read small XML/catalogue metadata; large file content should use `open`.
    pub async fn bytes(&self, url: Url, accept: &str) -> Result<Bytes> {
        let response = self.response(url, accept).await?;
        let mut length = BodyLength::new(response.headers())?;
        let mut body = response.into_body();
        let mut bytes = Vec::new();
        loop {
            let buffer = body.read().await.map_err(error::io)?;
            if buffer.is_empty() {
                break;
            }
            length.push(buffer.len())?;
            for part in buffer {
                bytes.extend_from_slice(&part);
            }
        }
        length.finish()?;
        Ok(bytes.into())
    }
}

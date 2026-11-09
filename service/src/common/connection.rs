use super::{error, url_util};
use http::{HeaderMap, HeaderValue, Request, header::HeaderName};
use open_arrow_access_core::Result;
use opendal::{Buffer, HttpTransporter, OperationContext};
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
}

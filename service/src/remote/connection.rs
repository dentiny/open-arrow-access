use super::error::{OP, invalid};
use http::{HeaderMap, HeaderValue};
use open_arrow_access_core::Result;
use opendal::{HttpTransporter, OperationContext};
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
        let origin = crate::http_client::validation::validate_origin(origin, OP)?;
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
        let name = http::header::HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| invalid("invalid header name"))?;
        let mut value =
            HeaderValue::from_str(value).map_err(|_| invalid("invalid header value"))?;
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
        if path.starts_with('/')
            || path.contains(['?', '#', '\\'])
            || path.split('/').any(|s| matches!(s, "." | ".."))
        {
            return Err(invalid("expected relative provider path"));
        }
        let mut url = self
            .origin
            .join(path)
            .map_err(|_| invalid("invalid provider path"))?;
        if url.origin() != self.origin.origin() {
            return Err(invalid("path changed provider origin"));
        }
        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query.iter().copied());
        }
        Ok(url)
    }
}

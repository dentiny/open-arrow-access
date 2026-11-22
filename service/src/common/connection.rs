use super::{
    ApiDocument, MetadataLink, error, resolve_download_url,
    transport::{self, AuthScope},
    url_util, validation,
};
use crate::{ResolvedFileSet, http_client::HttpClient};

mod reader;
use http::{HeaderMap, HeaderValue, Request, header::HeaderName};
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadOptions, ByteReadRequest, ByteReader, Bytes, Capability,
    Result, async_trait,
};
use open_arrow_access_storage::{HttpConcurrencyLimitLayer, StorageConfig};
use opendal::{Buffer, HttpTransporter, OperationContext, Operator, raw::Layer};
use url::Url;

use crate::file_manifest::FileSpec;

/// Where an origin-scoped API credential is sent.
#[derive(Clone, Copy, Debug)]
pub enum ApiKey<'a> {
    /// Send a sensitive HTTP header with this name.
    Header(&'a str),
    /// Send a URL query parameter with this name.
    Query(&'a str),
}

/// HTTP settings. Debug output never includes keys or signed URLs.
#[derive(Clone)]
pub struct Connection {
    pub(super) origin: Url,
    pub(super) headers: HeaderMap,
    pub(super) query: Vec<(String, String)>,
    operator: Operator,
    config: StorageConfig,
    http_concurrency_limit: Option<HttpConcurrencyLimitLayer>,
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
        let origin = crate::http_client::resolve_origin(origin, error::OP)?;
        let context = OperationContext::new().with_http_transport(HttpTransporter::new(
            crate::http_client::new_single_hop_transport()?,
        ));
        let operator =
            Operator::new(opendal_service_http::Http::default().endpoint(origin.as_str()))
                .map_err(|error| crate::http_client::transport_error(error::OP, error))?
                .with_context(context);
        Ok(Self {
            origin,
            headers: HeaderMap::new(),
            query: vec![],
            operator,
            config: Default::default(),
            http_concurrency_limit: None,
        })
    }
    /// Override transport, for custom TLS/proxy configuration or deterministic tests.
    /// The inner transport must return single-hop responses; redirects are followed by the provider transport.
    pub fn with_context(mut self, context: OperationContext) -> Self {
        self.operator = self.operator.with_context(context);
        self
    }
    /// Add an OpenDAL layer to the connection's HTTP operator.
    /// Layers are shared by connection clones and retained across `with_context`.
    /// API requests and file reads use the operator's composed HTTP context;
    /// OpenDAL operation hooks (such as read/stat wrappers) do not intercept
    /// these direct HTTP requests. Use HTTP context layers for request policies.
    ///
    /// ```no_run
    /// use std::time::Duration;
    /// use open_arrow_access_service::common::Connection;
    /// use open_arrow_access_storage::HttpRequestRateLimitLayer;
    /// # fn main() -> open_arrow_access_core::Result<()> {
    /// let limit = HttpRequestRateLimitLayer::new(Duration::from_secs(3))?;
    /// let connection = Connection::new("https://example.com")?.with_layer(limit);
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_layer<L: Layer>(mut self, layer: L) -> Self {
        self.operator = self.operator.layer(layer);
        self
    }
    /// Share a total HTTP request budget with other connections or OpenDAL operators.
    /// Clones, file downloads and context replacements retain the same budget.
    pub fn with_http_concurrency_limit(mut self, limit: HttpConcurrencyLimitLayer) -> Self {
        self.http_concurrency_limit = Some(limit);
        self
    }

    fn http_client(&self) -> HttpClient {
        let mut operator = self.operator.clone();
        if let Some(limit) = &self.http_concurrency_limit {
            operator = operator.layer(limit.clone());
        }
        HttpClient {
            context: transport::follow_redirects(operator.context().clone()),
            config: self.config.clone(),
        }
    }

    /// Configure retries and per-attempt/stream-read timeouts for this connection.
    /// Preserved when replacing the transport with `with_context`.
    pub fn with_http_config(mut self, config: StorageConfig) -> Result<Self> {
        config.validate()?;
        self.config = config;
        Ok(self)
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
    /// Set or replace an origin-scoped API key, rejecting an empty key before I/O.
    /// Header values are sensitive and credentials are omitted from Debug output.
    /// Keys are copied verbatim: whitespace is neither trimmed nor rejected.
    pub fn with_api_key(self, place: ApiKey<'_>, key: &str) -> Result<Self> {
        self.with_api_key_message(place, key, "API key must not be empty")
    }

    pub(crate) fn with_api_key_message(
        self,
        place: ApiKey<'_>,
        key: &str,
        empty_message: &str,
    ) -> Result<Self> {
        if key.is_empty() {
            return Err(error::invalid(empty_message));
        }
        match place {
            ApiKey::Header(name) => self.with_header(name, key),
            ApiKey::Query(name) => Ok(self.with_query(name, key)),
        }
    }

    /// Scoped query authentication, e.g. GovInfo's `api_key`.
    /// Replaces any previously configured value for the same parameter.
    pub fn with_query(mut self, name: &str, value: &str) -> Self {
        self.query.retain(|(key, _)| key != name);
        self.query.push((name.into(), value.into()));
        self
    }
    pub fn url(&self, path: &str, query: &[(&str, &str)]) -> Result<Url> {
        url_util::provider_url(&self.origin, path, query)
    }

    fn request(&self, url: &Url, accept: &str) -> Result<Request<Buffer>> {
        let mut request = HttpClient::get(url.as_str(), accept, error::OP)?;
        request.extensions_mut().insert(AuthScope {
            origin: self.origin.clone(),
            headers: self.headers.clone(),
            query: self.query.clone(),
        });
        Ok(request)
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
        let url = resolve_download_url(url.as_str())?;
        self.http_client()
            .bytes(self.request(&url, accept)?, error::OP)
            .await
    }
    /// Open an absolute URL, preserving encoded paths and signatures byte-for-byte.
    /// Readers support exact ranges for a known identity length with
    /// `Accept-Ranges: bytes`. Strong ETags pin requests to that version.
    /// Range-capable inputs with a known length use bounded reads for sequential
    /// output, avoiding an idle response holding a permit needed by range reads.
    pub async fn open(&self, url: &str, options: ByteReadOptions) -> Result<Box<dyn ByteReader>> {
        reader::open(self.clone(), resolve_download_url(url)?, options).await
    }
    /// Resolve file links into lazy downloads, retaining the full JSON as metadata.
    /// The resolver owns provider identity/availability checks.
    pub async fn resolve_json_files(
        &self,
        path: &str,
        query: &[(&str, &str)],
        resolve: impl FnOnce(&ApiDocument) -> Result<Vec<MetadataLink>> + Send,
    ) -> Result<RemoteFiles> {
        let document = self.json(path, query).await?;
        let files = resolve(&document)?;
        self.files(files, document)
    }

    /// Bind a validated provider manifest to lazy byte reads. Logical names remain opaque.
    pub fn files<M>(&self, files: Vec<MetadataLink>, metadata: M) -> Result<RemoteFiles<M>> {
        validation::validate_downloads(&files)?;
        ResolvedFileSet::from_files(
            self.clone(),
            files.into_iter().map(FileSpec::from),
            metadata,
        )
        .map_err(error::file_namespace)
    }
}

/// A manifest's provider metadata alongside a fixed namespace of HTTP files.
pub type RemoteFiles<M = ApiDocument> = ResolvedFileSet<Connection, M>;

#[async_trait]
impl Accessor for Connection {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: true,
            read_parallel: false,
            list: false,
        }
    }
    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        Connection::open(self, request.object.locator(), request.options).await
    }
}

#[async_trait]
impl ByteAccessor for Connection {}

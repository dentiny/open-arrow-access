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
use open_arrow_access_storage::StorageConfig;
use opendal::{Buffer, HttpTransporter, OperationContext};
use url::Url;

use crate::file_manifest::FileSpec;

/// HTTP settings. Debug output never includes keys or signed URLs.
#[derive(Clone)]
pub struct Connection {
    pub(super) origin: Url,
    pub(super) headers: HeaderMap,
    pub(super) query: Vec<(String, String)>,
    client: HttpClient,
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
        let context = transport::follow_redirects(OperationContext::new().with_http_transport(
            HttpTransporter::new(crate::http_client::new_single_hop_transport()?),
        ));
        Ok(Self {
            origin,
            headers: HeaderMap::new(),
            query: vec![],
            client: HttpClient {
                context,
                config: Default::default(),
            },
        })
    }
    /// Override transport, for custom TLS/proxy configuration or deterministic tests.
    /// The inner transport must return single-hop responses; redirects are followed by the provider transport.
    pub fn with_context(mut self, context: OperationContext) -> Self {
        self.client.context = transport::follow_redirects(context);
        self
    }
    /// Configure retries and per-attempt/stream-read timeouts for this connection.
    /// Preserved when replacing the transport with `with_context`.
    pub fn with_http_config(mut self, config: StorageConfig) -> Result<Self> {
        config.validate()?;
        self.client.config = config;
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
        self.client
            .bytes(self.request(&url, accept)?, error::OP)
            .await
    }
    /// Open an absolute URL, preserving encoded paths and signatures byte-for-byte.
    /// The sequential response is retained lazily. Readers additionally support exact
    /// ranges when it declares `Accept-Ranges: bytes`, an identity representation and
    /// a known length. Strong ETags pin subsequent range requests to that version.
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

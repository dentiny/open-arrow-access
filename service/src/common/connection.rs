use super::{
    ApiDocument, MetadataLink, error, resolve_download_url,
    transport::{self, AuthScope},
    url_util, validation,
};
use crate::{ResolvedFileSet, http_body::BodyReader, http_client::HttpClient};
use futures::stream;
use http::{HeaderMap, HeaderValue, Request, header::HeaderName};
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteMetadata, ByteReadOptions, ByteReadRequest, ByteReader, Bytes,
    Capability, Result, StreamReader, async_trait,
};
use open_arrow_access_storage::NamedFileAccessor;
use opendal::{Buffer, HttpBody, HttpTransporter, OperationContext};
use url::Url;

use crate::file_manifest::FileSpec;

const DEFAULT_CHUNK_SIZE: usize = 2 * 1024 * 1024;

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
            client: HttpClient { context },
        })
    }
    /// Override transport, for custom TLS/proxy configuration or deterministic tests.
    /// The inner transport must return single-hop responses; redirects are followed by the provider transport.
    pub fn with_context(mut self, context: OperationContext) -> Self {
        self.client.context = transport::follow_redirects(context);
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

    fn request(&self, url: &Url, accept: &str) -> Result<Request<Buffer>> {
        let mut request = HttpClient::get(url.as_str(), accept, error::OP)?;
        request.extensions_mut().insert(AuthScope {
            origin: self.origin.clone(),
            headers: self.headers.clone(),
            query: self.query.clone(),
        });
        Ok(request)
    }

    pub(super) async fn response(
        &self,
        url: Url,
        accept: &str,
    ) -> Result<http::Response<HttpBody>> {
        let url = resolve_download_url(url.as_str())?;
        let request = self.request(&url, accept)?;
        self.client.response(request, error::OP).await
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
    pub async fn open(&self, url: &str, options: ByteReadOptions) -> Result<Box<dyn ByteReader>> {
        let response = self.response(resolve_download_url(url)?, "*/*").await?;
        let version = response
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let chunk_size = options
            .io_chunk_size
            .map_or(DEFAULT_CHUNK_SIZE, |s| s.get());
        let body = BodyReader::new(response, error::OP)?;
        let content_length = body.content_length();
        let state = (body, Bytes::new());
        let chunks = stream::try_unfold(state, move |(mut body, mut pending)| async move {
            if pending.is_empty() {
                let Some(buffer) = body.next().await? else {
                    return Ok(None);
                };
                pending = buffer.to_bytes();
            }
            let chunk = pending.split_to(pending.len().min(chunk_size));
            Ok(Some((chunk, (body, pending))))
        });
        Ok(Box::new(StreamReader::new(
            chunks,
            ByteMetadata {
                source: None,
                content_length,
                version,
            },
        )))
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
        let files = NamedFileAccessor::new(
            self.clone(),
            files
                .into_iter()
                .map(FileSpec::from)
                .map(FileSpec::into_mapping)
                .collect(),
        )
        .map_err(error::file_namespace)?;
        Ok(ResolvedFileSet::new(files, metadata))
    }
}

/// A manifest's provider metadata alongside a fixed namespace of sequential files.
pub type RemoteFiles<M = ApiDocument> = ResolvedFileSet<Connection, M>;

#[async_trait]
impl Accessor for Connection {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
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

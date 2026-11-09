use super::{
    ApiDocument, MetadataLink, error, resolve_download_url,
    transport::{self, AuthScope},
    url_util, validation,
};
use crate::ResolvedFileSet;
use futures::stream;
use http::{HeaderMap, HeaderValue, Request, header::HeaderName};
use open_arrow_access_core::{
    Accessor, ByteMetadata, ByteReadOptions, ByteReadRequest, ByteReader, Bytes, Capability,
    Result, StreamReader, async_trait,
};
use open_arrow_access_storage::NamedFileAccessor;
use opendal::{Buffer, HttpBody, HttpTransporter, OperationContext, raw::oio::ReadStream};
use url::Url;

const DEFAULT_CHUNK_SIZE: usize = 2 * 1024 * 1024;

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
        let origin = crate::http_client::resolve_origin(origin, error::OP)?;
        let context = transport::follow_redirects(OperationContext::new().with_http_transport(
            HttpTransporter::new(crate::http_client::new_single_hop_transport()?),
        ));
        Ok(Self {
            origin,
            headers: HeaderMap::new(),
            query: vec![],
            context,
        })
    }

    /// Override transport, for custom TLS/proxy configuration or deterministic tests.
    /// The inner transport must return single-hop responses; redirects are followed by the provider transport.
    pub fn with_context(mut self, context: OperationContext) -> Self {
        self.context = transport::follow_redirects(context);
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
        let mut request = Request::get(url.as_str())
            .header("accept", accept)
            .body(Buffer::new())
            .map_err(error::request)?;
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
        let response = self
            .context
            .http_transport()
            .fetch(request)
            .await
            .map_err(error::io)?;
        if response.status() != http::StatusCode::OK {
            return Err(error::http(response.status()));
        }
        Ok(response)
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

    /// Open an absolute URL, preserving encoded paths and signatures byte-for-byte.
    pub async fn open(&self, url: &str, options: ByteReadOptions) -> Result<Box<dyn ByteReader>> {
        let response = self.response(resolve_download_url(url)?, "*/*").await?;
        let length = BodyLength::new(response.headers())?;
        let content_length = length.expected;
        let version = response
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let chunk_size = options
            .io_chunk_size
            .map_or(DEFAULT_CHUNK_SIZE, |s| s.get());
        let state = (response.into_body(), Bytes::new(), length);
        let chunks = stream::try_unfold(
            state,
            move |(mut body, mut pending, mut length)| async move {
                if pending.is_empty() {
                    let buffer = body.read().await.map_err(error::io)?;
                    if buffer.is_empty() {
                        length.finish()?;
                        return Ok(None);
                    }
                    pending = buffer.to_bytes();
                    length.push(pending.len())?;
                }
                let chunk = pending.split_to(pending.len().min(chunk_size));
                Ok(Some((chunk, (body, pending, length))))
            },
        );
        Ok(Box::new(StreamReader::new(
            chunks,
            ByteMetadata {
                source: None,
                content_length,
                version,
            },
        )))
    }

    /// Bind a validated provider manifest to lazy byte reads. Logical names remain opaque.
    pub fn files<M>(&self, files: Vec<MetadataLink>, metadata: M) -> Result<RemoteFiles<M>> {
        validation::validate_downloads(&files)?;
        let files = NamedFileAccessor::new(
            self.clone(),
            files
                .into_iter()
                .map(|file| (file.file_name, file.url.into()))
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

/// Track response length while buffered and streaming reads consume the body.
struct BodyLength {
    expected: Option<u64>,
    read: u64,
}
impl BodyLength {
    fn new(headers: &HeaderMap) -> Result<Self> {
        let expected = headers
            .get("content-length")
            .map(|value| {
                value
                    .to_str()
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok())
                    .ok_or_else(|| error::decode("invalid Content-Length"))
            })
            .transpose()?;
        Ok(Self { expected, read: 0 })
    }

    fn push(&mut self, bytes: usize) -> Result<()> {
        self.read = self
            .read
            .checked_add(bytes as u64)
            .ok_or_else(|| error::decode("body length overflow"))?;
        validation::validate_body_progress(self.expected, self.read)
    }

    fn finish(&self) -> Result<()> {
        validation::validate_body_complete(self.expected, self.read)
    }
}

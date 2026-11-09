//! Provider HTTP connections, exact response bytes, and lazy sequential downloads.
//!
//! No background requests, retries, rate-limit bypass, or automatic page walks.
//! Credentials are scoped to the configured origin and stripped on cross-origin redirects.

use crate::ResolvedFileSet;
use futures::stream;
use http::{HeaderMap, HeaderValue, Request};
use open_arrow_access_core::{
    Accessor, ByteMetadata, ByteReadRequest, ByteReader, Bytes, Capability, Error, ErrorKind,
    Result, StreamReader, async_trait,
};
use open_arrow_access_storage::NamedFileAccessor;
use opendal::{Buffer, HttpBody, HttpTransporter, OperationContext, raw::oio::ReadStream};
use serde_json::Value;
use url::Url;

pub use crate::http_client::HttpStatusError;

const OP: &str = "provider_http";

pub(crate) fn invalid(message: &str) -> Error {
    Error::new(ErrorKind::InvalidInput, OP, message)
}
pub(crate) fn decode(message: &str) -> Error {
    Error::new(ErrorKind::Decode, OP, message)
}
fn io(error: opendal::Error) -> Error {
    crate::http_client::transport_error(OP, error)
}

/// Validate an absolute download URL without altering its signed query string.
pub(crate) fn download_url(value: &str) -> Result<Url> {
    let url = Url::parse(value).map_err(|_| decode("invalid download URL"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(decode("expected credential-free HTTP(S) download URL"));
    }
    Ok(url)
}

/// HTTP settings. Debug output never includes keys or signed URLs.
#[derive(Clone)]
pub struct Connection {
    origin: Url,
    headers: HeaderMap,
    query: Vec<(String, String)>,
    context: OperationContext,
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
    async fn response(&self, url: Url, accept: &str) -> Result<http::Response<HttpBody>> {
        let mut url = download_url(url.as_str())?;
        let mut authorized = url.origin() == self.origin.origin();
        if authorized && !self.query.is_empty() {
            url.query_pairs_mut()
                .extend_pairs(self.query.iter().map(|(k, v)| (k, v)));
        }
        for _ in 0..=10 {
            let mut request = Request::get(url.as_str())
                .header("accept", accept)
                .body(Buffer::new())
                .map_err(|e| invalid("invalid HTTP request").with_source(e))?;
            if authorized {
                request.headers_mut().extend(self.headers.clone());
            }
            let response = self
                .context
                .http_transport()
                .fetch(request)
                .await
                .map_err(io)?;
            let status = response.status();
            if matches!(status.as_u16(), 301 | 302 | 303 | 307 | 308) {
                let location = response
                    .headers()
                    .get("location")
                    .and_then(|v| v.to_str().ok())
                    .ok_or_else(|| decode("redirect lacks Location"))?;
                let next = download_url(
                    url.join(location)
                        .map_err(|_| decode("invalid redirect"))?
                        .as_str(),
                )?;
                if url.scheme() == "https" && next.scheme() != "https" {
                    return Err(decode("HTTPS downgrade redirect"));
                }
                authorized = authorized && next.origin() == url.origin();
                url = next;
                continue;
            }
            crate::http_client::check_status(status, response.headers(), OP)?;
            return Ok(response);
        }
        Err(decode("too many redirects"))
    }
    /// Exact upstream JSON bytes plus a parsed view. One call performs one page request.
    pub async fn json(&self, path: &str, query: &[(&str, &str)]) -> Result<ApiDocument> {
        self.json_url(self.url(path, query)?).await
    }
    /// Fetch an API continuation URL only if it remains within the configured origin.
    pub async fn json_url(&self, url: Url) -> Result<ApiDocument> {
        if url.origin() != self.origin.origin() {
            return Err(invalid("API continuation changed origin"));
        }
        let bytes = self.bytes(url, "application/json").await?;
        let value = serde_json::from_slice(&bytes)
            .map_err(|e| decode("invalid JSON response").with_source(e))?;
        Ok(ApiDocument { bytes, value })
    }
    /// Read small XML/catalogue metadata; large file content should use `open`.
    pub async fn bytes(&self, url: Url, accept: &str) -> Result<Bytes> {
        let mut body = self.response(url, accept).await?.into_body();
        let mut bytes = Vec::new();
        loop {
            let buffer = body.read().await.map_err(io)?;
            if buffer.is_empty() {
                break;
            }
            for part in buffer {
                bytes.extend_from_slice(&part);
            }
        }
        Ok(bytes.into())
    }
    /// Bind a validated provider manifest to lazy byte reads. Logical names remain opaque.
    pub fn files<M>(&self, files: Vec<(String, String)>, metadata: M) -> Result<RemoteFiles<M>> {
        for (_, url) in &files {
            download_url(url)?;
        }
        let files = NamedFileAccessor::new(
            RemoteHttpAccessor(self.clone()),
            files
                .into_iter()
                .map(|(name, url)| (name, url.into()))
                .collect(),
        )
        .map_err(|e| decode("invalid file namespace").with_source(e))?;
        Ok(ResolvedFileSet::new(files, metadata))
    }
    /// Open an absolute URL, preserving encoded paths and signatures byte-for-byte.
    pub async fn open(
        &self,
        url: &str,
        options: open_arrow_access_core::ByteReadOptions,
    ) -> Result<Box<dyn ByteReader>> {
        let response = self.response(download_url(url)?, "*/*").await?;
        let length = response
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .map(str::parse::<u64>)
            .transpose()
            .map_err(|_| decode("invalid Content-Length"))?;
        let version = response
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let chunk_size = options.io_chunk_size.map_or(64 * 1024, |s| s.get());
        let state = (response.into_body(), Bytes::new(), 0u64);
        let chunks =
            stream::try_unfold(state, move |(mut body, mut pending, mut read)| async move {
                if pending.is_empty() {
                    let buffer = body.read().await.map_err(io)?;
                    if buffer.is_empty() {
                        if length.is_some_and(|len| read != len) {
                            return Err(decode("truncated HTTP body"));
                        }
                        return Ok(None);
                    }
                    pending = buffer.to_bytes();
                    read = read
                        .checked_add(pending.len() as u64)
                        .ok_or_else(|| decode("body length overflow"))?;
                    if length.is_some_and(|len| read > len) {
                        return Err(decode("HTTP body exceeds Content-Length"));
                    }
                }
                let chunk = pending.split_to(pending.len().min(chunk_size));
                Ok(Some((chunk, (body, pending, read))))
            });
        Ok(Box::new(StreamReader::new(
            chunks,
            ByteMetadata {
                source: None,
                content_length: length,
                version,
            },
        )))
    }
}

/// Raw upstream response and parsed fields; unknown fields and exact raw bytes survive.
#[derive(Clone, Debug)]
pub struct ApiDocument {
    bytes: Bytes,
    value: Value,
}
impl ApiDocument {
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }
    pub fn value(&self) -> &Value {
        &self.value
    }
    pub fn reader(&self) -> Box<dyn ByteReader> {
        let bytes = self.bytes.clone();
        let length = bytes.len() as u64;
        Box::new(StreamReader::new(
            stream::iter((!bytes.is_empty()).then_some(Ok(bytes))),
            ByteMetadata {
                source: None,
                content_length: Some(length),
                version: None,
            },
        ))
    }
}
/// A manifest's provider metadata alongside a fixed namespace of sequential files.
pub type RemoteFiles<M = ApiDocument> = ResolvedFileSet<RemoteHttpAccessor, M>;
#[derive(Clone, Debug)]
pub struct RemoteHttpAccessor(Connection);
#[async_trait]
impl Accessor for RemoteHttpAccessor {
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
        self.0.open(request.object.locator(), request.options).await
    }
}

/// Encode one provider path component, including slashes in opaque IDs/tokens.
pub fn segment(value: &str) -> Result<String> {
    if value.is_empty() || matches!(value, "." | "..") {
        return Err(invalid("empty or dot identifier"));
    }
    let mut url = Url::parse("https://encode.invalid/").unwrap();
    url.path_segments_mut().unwrap().push(value);
    Ok(url.path()[1..].to_owned())
}

/// Required JSON field validation shared by provider-specific schema mappings.
pub fn text<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| decode(&format!("missing or invalid {field}")))
}
/// Required JSON array validation; missing and null are not silently treated as empty.
pub fn array<'a>(value: &'a Value, field: &str) -> Result<&'a Vec<Value>> {
    value
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| decode(&format!("missing or invalid {field}")))
}

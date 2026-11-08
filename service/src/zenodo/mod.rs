//! Published Zenodo records and their attached files.

use std::{collections::HashSet, num::NonZeroU64};

use futures::stream;
use http::{Request, StatusCode};
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, ByteReader, Capability, Error, ErrorKind, ListRequest,
    ObjectRef, ObjectStream, Result, async_trait,
};
use open_arrow_access_storage::OpenDalAccessor;
use opendal::{Buffer, OperationContext, Operator, raw::oio::ReadStream};
use serde::Deserialize;
use url::Url;

const DEFAULT_METADATA_LIMIT: usize = 4 * 1024 * 1024;

/// Connection settings. Construction performs no network requests.
#[derive(Clone, Debug)]
pub struct ZenodoService {
    endpoint: Url,
    context: OperationContext,
    metadata_limit: usize,
}

impl ZenodoService {
    pub fn new() -> Result<Self> {
        Self::with_endpoint("https://zenodo.org")
    }

    /// Select an origin, for example `https://sandbox.zenodo.org`.
    pub fn with_endpoint(endpoint: &str) -> Result<Self> {
        let endpoint = Url::parse(endpoint).map_err(|_| invalid("invalid endpoint"))?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || endpoint.path() != "/"
        {
            return Err(invalid("expected an HTTP(S) origin without credentials"));
        }
        opendal::install_default();
        Ok(Self {
            endpoint,
            context: OperationContext::new(),
            metadata_limit: DEFAULT_METADATA_LIMIT,
        })
    }

    pub fn with_metadata_limit(mut self, bytes: usize) -> Self {
        self.metadata_limit = bytes;
        self
    }

    /// Resolve one published record. Membership is fetched once; attachment
    /// bytes are read lazily through the returned accessor.
    pub async fn record(&self, id: NonZeroU64) -> Result<ZenodoAccessor> {
        let url = self.endpoint.join(&format!("api/records/{id}")).unwrap();
        let request = Request::get(url.as_str())
            .header("accept", "application/json")
            .header("user-agent", "open-arrow-access/0.1.0")
            .body(Buffer::new())
            .map_err(|_| invalid("invalid metadata request"))?;
        let response = self
            .context
            .http_transport()
            .fetch(request)
            .await
            .map_err(io_error)?;
        let status = response.status();
        if status != StatusCode::OK {
            let kind = match status {
                StatusCode::NOT_FOUND => ErrorKind::NotFound,
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => ErrorKind::PermissionDenied,
                _ => ErrorKind::Io,
            };
            return Err(Error::new(
                kind,
                "resolve_zenodo",
                format!("metadata HTTP {status}"),
            ));
        }
        let mut body = response.into_body();
        let mut bytes = Vec::new();
        loop {
            let buffer = body.read().await.map_err(io_error)?;
            if buffer.is_empty() {
                break;
            }
            if buffer.len() > self.metadata_limit - bytes.len() {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "resolve_zenodo",
                    "record metadata exceeds configured byte limit",
                ));
            }
            for part in buffer {
                bytes.extend_from_slice(&part);
            }
        }
        let record: Record = serde_json::from_slice(&bytes)
            .map_err(|error| decode("invalid record metadata").with_source(error))?;
        if record.id != id.get() {
            return Err(decode("record ID differs from the requested version"));
        }
        let mut keys = HashSet::new();
        for file in &record.files {
            if !valid_key(&file.key) || !keys.insert(&file.key) {
                return Err(decode("invalid or duplicate attachment key"));
            }
        }
        // Use the resolved record's canonical content route, never a concept's
        // latest-version link or an arbitrary host supplied in metadata.
        let root = format!("api/records/{id}/files/");
        let operator = Operator::new(
            opendal_service_http::Http::default()
                .endpoint(self.endpoint.as_str().trim_end_matches('/'))
                .root(&root),
        )
        .map_err(io_error)?
        .with_context(self.context.clone());
        Ok(ZenodoAccessor {
            id,
            files: record.files,
            inner: OpenDalAccessor::new(operator),
        })
    }
}

#[derive(Deserialize)]
struct Record {
    id: u64,
    // Omission is not treated as an empty record (e.g. restricted metadata).
    files: Vec<ZenodoFile>,
}

/// Attachment metadata as supplied by Zenodo. Checksums are exposed, not verified.
#[derive(Clone, Debug, Deserialize)]
pub struct ZenodoFile {
    pub key: String,
    pub size: u64,
    pub checksum: String,
}

/// A resolved record's flat attachment namespace. Compose with existing format
/// and decompression accessors to decode selected files.
#[derive(Clone, Debug)]
pub struct ZenodoAccessor {
    id: NonZeroU64,
    files: Vec<ZenodoFile>,
    inner: OpenDalAccessor,
}

impl ZenodoAccessor {
    pub fn id(&self) -> NonZeroU64 {
        self.id
    }
    pub fn files(&self) -> &[ZenodoFile] {
        &self.files
    }
}

#[async_trait]
impl Accessor for ZenodoAccessor {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        Capability {
            list: true,
            ..self.inner.capability()
        }
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        let key = request.object.locator();
        if !self.files.iter().any(|file| file.key == key) {
            return Err(Error::new(
                ErrorKind::NotFound,
                "open_zenodo",
                "attachment not found in record",
            ));
        }
        self.inner
            .open(ByteReadRequest::new(format!("{key}/content")).with_options(request.options))
            .await
    }
}

#[async_trait]
impl ByteAccessor for ZenodoAccessor {
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        if !request.prefix.locator().is_empty() {
            return Err(invalid(
                "Zenodo attachments have a flat namespace; list the empty root",
            ));
        }
        Ok(Box::pin(stream::iter(
            self.files
                .iter()
                .map(|file| Ok(ObjectRef::new(file.key.clone())))
                .collect::<Vec<_>>(),
        )))
    }
}

fn valid_key(key: &str) -> bool {
    !key.is_empty()
        && key != "."
        && key != ".."
        && !key.contains(['/', '\\'])
        && !key.chars().any(char::is_control)
        && key.trim() == key
}

fn invalid(message: &str) -> Error {
    Error::new(ErrorKind::InvalidInput, "configure_zenodo", message)
}
fn decode(message: &str) -> Error {
    Error::new(ErrorKind::Decode, "resolve_zenodo", message)
}
fn io_error(error: opendal::Error) -> Error {
    let kind = match error.kind() {
        opendal::ErrorKind::NotFound => ErrorKind::NotFound,
        opendal::ErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
        _ => ErrorKind::Io,
    };
    Error::new(kind, "resolve_zenodo", "Zenodo HTTP operation failed").with_source(error)
}

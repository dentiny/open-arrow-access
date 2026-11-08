//! Published Zenodo records and their attached files.

mod validation;

use std::num::NonZeroU64;

use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, ByteReader, Capability, Error, ErrorKind, ListRequest,
    ObjectStream, Result, async_trait,
};
use open_arrow_access_storage::{NamedFileAccessor, OpenDalAccessor};
use opendal::Operator;

use serde::Deserialize;
use url::Url;

use crate::http_client::HttpClient;
use validation::validate_endpoint;

const DEFAULT_METADATA_LIMIT: usize = 4 * 1024 * 1024;

/// Connection settings. Construction performs no network requests.
#[derive(Clone, Debug)]
pub struct ZenodoService {
    endpoint: Url,
    client: HttpClient,
    metadata_limit: usize,
}

impl ZenodoService {
    pub fn new() -> Result<Self> {
        Self::with_endpoint("https://zenodo.org")
    }

    /// Select an origin, for example `https://sandbox.zenodo.org`.
    pub fn with_endpoint(endpoint: &str) -> Result<Self> {
        let endpoint = Url::parse(endpoint).map_err(|_| invalid("invalid endpoint"))?;
        validate_endpoint(&endpoint)?;
        Ok(Self {
            endpoint,
            client: HttpClient::new()?,
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
        let record: Record = self
            .client
            .json(url.as_str(), None, self.metadata_limit, "resolve_zenodo")
            .await?;
        if record.id != id.get() {
            return Err(decode("record ID differs from the requested version"));
        }
        if record.files.iter().any(|file| !valid_key(&file.key)) {
            return Err(decode("invalid attachment key"));
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
        .with_context(self.client.context.clone());
        let inner = NamedFileAccessor::new(
            OpenDalAccessor::new(operator),
            record
                .files
                .iter()
                .map(|file| (file.key.clone(), format!("{}/content", file.key).into()))
                .collect(),
        )
        .map_err(|error| decode("invalid attachment membership").with_source(error))?;
        Ok(ZenodoAccessor {
            id,
            files: record.files,
            inner,
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
    inner: NamedFileAccessor<OpenDalAccessor>,
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
        self.inner.capability()
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        self.inner.open(request).await
    }
}

#[async_trait]
impl ByteAccessor for ZenodoAccessor {
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        self.inner.list(request).await
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

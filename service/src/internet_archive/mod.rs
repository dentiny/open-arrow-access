//! Public Internet Archive item metadata and attached file bytes.

mod error;
mod validation;

use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, ByteReader, Capability, ListRequest, ObjectStream,
    Result, async_trait,
};
use open_arrow_access_storage::{NamedFileAccessor, OpenDalAccessor};
use serde::Deserialize;
use url::Url;

use crate::http_client::{HttpClient, validation::validate_origin};
use error::membership_error;
use validation::{validate_identifier, validate_item};

const DEFAULT_METADATA_LIMIT: usize = 4 * 1024 * 1024;
const CONFIGURE: &str = "configure_internet_archive";
const RESOLVE: &str = "resolve_internet_archive";

/// Resolve one public item at an Internet Archive origin. Construction performs no I/O.
#[derive(Clone, Debug)]
pub struct InternetArchiveService {
    endpoint: Url,
    client: HttpClient,
    metadata_limit: usize,
}

impl InternetArchiveService {
    pub fn new() -> Result<Self> {
        Self::with_endpoint("https://archive.org")
    }

    pub fn with_endpoint(endpoint: &str) -> Result<Self> {
        Ok(Self {
            endpoint: validate_origin(endpoint, CONFIGURE)?,
            client: HttpClient::new()?,
            metadata_limit: DEFAULT_METADATA_LIMIT,
        })
    }

    pub fn with_metadata_limit(mut self, bytes: usize) -> Self {
        self.metadata_limit = bytes;
        self
    }

    /// Fetch membership once. File bytes are opened lazily through canonical
    /// `/download/{identifier}/{name}` URLs, which may redirect to archive data nodes.
    pub async fn item(&self, identifier: &str) -> Result<InternetArchiveAccessor> {
        validate_identifier(identifier)?;
        let url = self
            .endpoint
            .join(&format!("metadata/{identifier}"))
            .unwrap();
        let response = self
            .client
            .json(url.as_str(), None, self.metadata_limit, RESOLVE)
            .await?;
        let files = validate_item(response, identifier)?;
        let storage =
            self.client
                .accessor(&self.endpoint, &format!("download/{identifier}/"), &[])?;
        let inner = NamedFileAccessor::new(
            storage,
            files
                .iter()
                .map(|file| (file.name.clone(), file.name.clone().into()))
                .collect(),
        )
        .map_err(membership_error)?;
        Ok(InternetArchiveAccessor {
            identifier: identifier.into(),
            files,
            inner,
        })
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ItemResponse {
    Item(Item),
    Missing([(); 0]),
}

#[derive(Deserialize)]
struct Item {
    metadata: Option<ItemMetadata>,
    files: Option<Vec<InternetArchiveFile>>,
    error: Option<String>,
    #[serde(default)]
    is_dark: bool,
}

#[derive(Deserialize)]
struct ItemMetadata {
    identifier: String,
}

/// Upstream file attributes. Missing sizes and checksums are retained as None;
/// checksums are exposed without verification.
#[derive(Clone, Debug, Deserialize)]
pub struct InternetArchiveFile {
    pub name: String,
    #[serde(default, deserialize_with = "validation::deserialize_size")]
    pub size: Option<u64>,
    pub format: Option<String>,
    pub source: Option<String>,
    pub md5: Option<String>,
}

/// Fixed item membership over independently owned HTTP byte readers.
/// Listing the empty root returns exact names, including nested paths, in API order.
#[derive(Clone, Debug)]
pub struct InternetArchiveAccessor {
    identifier: String,
    files: Vec<InternetArchiveFile>,
    inner: NamedFileAccessor<OpenDalAccessor>,
}

impl InternetArchiveAccessor {
    pub fn identifier(&self) -> &str {
        &self.identifier
    }
    pub fn files(&self) -> &[InternetArchiveFile] {
        &self.files
    }
}

#[async_trait]
impl Accessor for InternetArchiveAccessor {
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
impl ByteAccessor for InternetArchiveAccessor {
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        self.inner.list(request).await
    }
}

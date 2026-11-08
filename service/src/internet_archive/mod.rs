//! Public Internet Archive item metadata and attached file bytes.

mod error;
mod validation;

use open_arrow_access_core::Result;
use open_arrow_access_storage::{NamedFileAccessor, OpenDalAccessor};
use serde::{Deserialize, Deserializer, de::Error as _};
use url::Url;

use crate::ResolvedFileSet;
use crate::http_client::{HttpClient, validation::validate_origin};
use error::membership_error;
use validation::{extract_items_and_validate, validate_identifier};

const CONFIGURE: &str = "configure_internet_archive";
const RESOLVE: &str = "resolve_internet_archive";

/// Resolve one public item at an Internet Archive origin. Construction performs no I/O.
#[derive(Clone, Debug)]
pub struct InternetArchiveService {
    endpoint: Url,
    client: HttpClient,
}

impl InternetArchiveService {
    pub fn new() -> Result<Self> {
        Self::with_endpoint("https://archive.org")
    }

    pub fn with_endpoint(endpoint: &str) -> Result<Self> {
        Ok(Self {
            endpoint: validate_origin(endpoint, CONFIGURE)?,
            client: HttpClient::new()?,
        })
    }

    /// Fetch membership once. File bytes are opened lazily through canonical
    /// `/download/{identifier}/{name}` URLs, which may redirect to archive data nodes.
    pub async fn item(&self, identifier: &str) -> Result<InternetArchiveAccessor> {
        validate_identifier(identifier)?;
        let url = self
            .endpoint
            .join(&format!("metadata/{identifier}"))
            .unwrap();
        let response = self.client.json(url.as_str(), None, RESOLVE).await?;
        let files = extract_items_and_validate(response, identifier)?;
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
        Ok(ResolvedFileSet::new(
            inner,
            InternetArchiveMetadata {
                identifier: identifier.into(),
                files,
            },
        ))
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
    /// Item was hidden by Internet Archive and is not publicly accessible.
    #[serde(default, rename = "is_dark")]
    is_unavailable: bool,
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
    #[serde(default, deserialize_with = "deserialize_size")]
    pub size: Option<u64>,
    pub format: Option<String>,
    pub source: Option<String>,
    pub md5: Option<String>,
}

fn deserialize_size<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<u64>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Size {
        Number(u64),
        Text(String),
    }
    Option::<Size>::deserialize(deserializer)?
        .map(|size| match size {
            Size::Number(value) => Ok(value),
            Size::Text(value) => value.parse().map_err(D::Error::custom),
        })
        .transpose()
}

/// Fixed item membership over independently owned HTTP byte readers.
/// Listing the empty root returns exact names, including nested paths, in API order.
pub type InternetArchiveAccessor = ResolvedFileSet<OpenDalAccessor, InternetArchiveMetadata>;

/// Metadata retained from the resolved item.
#[derive(Clone, Debug)]
pub struct InternetArchiveMetadata {
    pub identifier: String,
    pub files: Vec<InternetArchiveFile>,
}

impl InternetArchiveAccessor {
    pub fn identifier(&self) -> &str {
        &self.metadata().identifier
    }

    pub fn files(&self) -> &[InternetArchiveFile] {
        &self.metadata().files
    }
}

//! Public Internet Archive item metadata and attached file bytes.

mod error;
mod validation;

use open_arrow_access_core::Result;
use open_arrow_access_storage::{OpenDalAccessor, StorageConfig};
use serde::{Deserialize, Deserializer, de::Error as _};

use crate::http_client::files::{HttpFileService, ResolvedFilesPlan};
use crate::{ResolvedFileSet, file_manifest::FileSpec};
use error::map_file_namespace_error;
use validation::{extract_items_and_validate, validate_identifier};

const CONFIGURE: &str = "configure_internet_archive";
const RESOLVE: &str = "resolve_internet_archive";

/// Resolve one public item at an Internet Archive origin. Construction performs no I/O.
#[derive(Clone, Debug)]
pub struct InternetArchiveService {
    http: HttpFileService,
}

impl InternetArchiveService {
    /// Apply the same HTTP policy to metadata and lazy file requests.
    pub fn with_http_config(mut self, config: StorageConfig) -> Result<Self> {
        self.http = self.http.with_http_config(config)?;
        Ok(self)
    }

    pub fn new() -> Result<Self> {
        Self::with_endpoint("https://archive.org")
    }

    pub fn with_endpoint(endpoint: &str) -> Result<Self> {
        Ok(Self {
            http: HttpFileService::new(endpoint, CONFIGURE, RESOLVE, map_file_namespace_error)?,
        })
    }

    /// Fetch membership once. File bytes are opened lazily through canonical
    /// `/download/{identifier}/{name}` URLs, which may redirect to archive data nodes.
    pub async fn item(&self, identifier: &str) -> Result<InternetArchiveAccessor> {
        validate_identifier(identifier)?;
        let url = self
            .http
            .endpoint()
            .join(&format!("metadata/{identifier}"))
            .unwrap();
        self.http
            .resolve_files(&url, |response| {
                let files = extract_items_and_validate(response, identifier)?;
                ResolvedFilesPlan::new(
                    format!("download/{identifier}/"),
                    files
                        .iter()
                        .map(|file| FileSpec {
                            name: file.name.clone(),
                            object: file.name.clone().into(),
                            content_length: None,
                        })
                        .collect(),
                    InternetArchiveMetadata {
                        identifier: identifier.into(),
                        files,
                    },
                )
                .map_err(map_file_namespace_error)
            })
            .await
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

/// List exact names, including nested paths, at the empty root in API order.
pub type InternetArchiveAccessor = ResolvedFileSet<OpenDalAccessor, InternetArchiveMetadata>;

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

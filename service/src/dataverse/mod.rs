//! Published Dataverse dataset versions and their original-download files.

mod error;
mod validation;

use open_arrow_access_core::Result;
use open_arrow_access_storage::OpenDalAccessor;
use serde::Deserialize;

use crate::http_client::files::{HttpFileService, ResolvedFilesPlan};
use crate::{ResolvedFileSet, file_manifest::FileSpec};
use error::map_file_namespace_error;
use validation::{validate_dataset, validate_request};

#[derive(Clone, Debug)]
pub struct DataverseService {
    http: HttpFileService,
}

impl DataverseService {
    /// Apply the same HTTP policy to metadata and lazy file requests.
    pub fn with_http_config(
        mut self,
        config: open_arrow_access_storage::StorageConfig,
    ) -> Result<Self> {
        self.http = self.http.with_http_config(config)?;
        Ok(self)
    }

    /// Configure a Dataverse installation origin without performing network I/O.
    pub fn new(endpoint: &str) -> Result<Self> {
        Ok(Self {
            http: HttpFileService::new(
                endpoint,
                "configure_dataverse",
                "resolve_dataverse",
                map_file_namespace_error,
            )?,
        })
    }

    /// Resolve one explicit published version, for example `doi:10.7910/DVN/ABC`,
    /// `1.0`. Attachment membership is cached; file bytes are read lazily.
    pub async fn dataset(&self, persistent_id: &str, version: &str) -> Result<DataverseAccessor> {
        let expected = validate_request(persistent_id, version)?;
        let mut url = self
            .http
            .endpoint()
            .join(&format!("api/v1/datasets/:persistentId/versions/{version}"))
            .unwrap();
        url.query_pairs_mut()
            .append_pair("persistentId", persistent_id)
            .append_pair("excludeFiles", "false")
            .append_pair("excludeMetadataBlocks", "true");
        self.http
            .resolve_files(&url, |response: Response| {
                validate_dataset(&response, persistent_id, expected)?;
                let files: Vec<_> = response
                    .data
                    .files
                    .into_iter()
                    .map(|file| DataverseFile {
                        name: if file.directory_label.is_empty() {
                            file.label
                        } else {
                            format!(
                                "{}/{}",
                                file.directory_label.trim_end_matches('/'),
                                file.label
                            )
                        },
                        id: file.data_file.id,
                        size: file
                            .data_file
                            .original_file_size
                            .unwrap_or(file.data_file.filesize),
                    })
                    .collect();
                let mappings = files
                    .iter()
                    .map(|file| FileSpec {
                        name: file.name.clone(),
                        object: file.id.to_string().into(),
                        content_length: Some(file.size),
                    })
                    .collect();
                Ok(ResolvedFilesPlan::new(
                    "api/access/datafile/",
                    mappings,
                    DataverseMetadata {
                        persistent_id: persistent_id.to_owned(),
                        version: version.to_owned(),
                        files,
                    },
                )
                .map_err(map_file_namespace_error)?
                .with_query(&[("format", "original")]))
            })
            .await
    }
}

/// Public manifest metadata for the original-download representation.
/// Logical names preserve the published labels, including tabular `.tab` labels.
#[derive(Clone, Debug)]
pub struct DataverseFile {
    pub name: String,
    pub id: u64,
    pub size: u64,
}

/// A published version's fixed file namespace and metadata.
pub type DataverseAccessor = ResolvedFileSet<OpenDalAccessor, DataverseMetadata>;

#[derive(Clone, Debug)]
pub struct DataverseMetadata {
    pub persistent_id: String,
    pub version: String,
    pub files: Vec<DataverseFile>,
}

impl DataverseAccessor {
    pub fn persistent_id(&self) -> &str {
        &self.metadata().persistent_id
    }
    pub fn version(&self) -> &str {
        &self.metadata().version
    }
    pub fn files(&self) -> &[DataverseFile] {
        &self.metadata().files
    }
}

#[derive(Deserialize)]
struct Response {
    status: String,
    data: DatasetVersion,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DatasetVersion {
    dataset_persistent_id: String,
    version_number: u64,
    version_minor_number: u64,
    version_state: String,
    files: Vec<FileMetadata>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FileMetadata {
    label: String,
    #[serde(default)]
    directory_label: String,
    restricted: bool,
    data_file: FileData,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FileData {
    id: u64,
    filesize: u64,
    tabular_data: bool,
    original_file_size: Option<u64>,
}

//! A published Dataverse dataset version exposed as independently readable files.

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
use validation::{validate_dataset, validate_request};

#[derive(Clone, Debug)]
pub struct DataverseService {
    endpoint: Url,
    client: HttpClient,
}

impl DataverseService {
    /// Configure a Dataverse installation origin without performing network I/O.
    pub fn new(endpoint: &str) -> Result<Self> {
        Ok(Self {
            endpoint: validate_origin(endpoint, "configure_dataverse")?,
            client: HttpClient::new()?,
        })
    }

    /// Resolve one explicit published version, for example `doi:10.7910/DVN/ABC`,
    /// `1.0`. Attachment membership is cached; file bytes are read lazily.
    pub async fn dataset(&self, persistent_id: &str, version: &str) -> Result<DataverseAccessor> {
        let expected = validate_request(persistent_id, version)?;
        let mut url = self
            .endpoint
            .join(&format!("api/v1/datasets/:persistentId/versions/{version}"))
            .unwrap();
        url.query_pairs_mut()
            .append_pair("persistentId", persistent_id)
            .append_pair("excludeFiles", "false")
            .append_pair("excludeMetadataBlocks", "true");
        let response: Response = self
            .client
            .json(url.as_str(), None, "resolve_dataverse")
            .await?;
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
        let inner = NamedFileAccessor::new(
            self.client
                .accessor(
                    &self.endpoint,
                    "api/access/datafile/",
                    &[("format", "original")],
                )?
                .with_content_lengths(files.iter().map(|file| (file.id.to_string(), file.size))),
            files
                .iter()
                .map(|file| (file.name.clone(), file.id.to_string().into()))
                .collect(),
        )
        .map_err(membership_error)?;
        Ok(DataverseAccessor {
            persistent_id: persistent_id.to_owned(),
            version: version.to_owned(),
            files,
            inner,
        })
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

#[derive(Clone, Debug)]
pub struct DataverseAccessor {
    persistent_id: String,
    version: String,
    files: Vec<DataverseFile>,
    inner: NamedFileAccessor<OpenDalAccessor>,
}

impl DataverseAccessor {
    pub fn persistent_id(&self) -> &str {
        &self.persistent_id
    }
    pub fn version(&self) -> &str {
        &self.version
    }
    pub fn files(&self) -> &[DataverseFile] {
        &self.files
    }
}

#[async_trait]
impl Accessor for DataverseAccessor {
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
impl ByteAccessor for DataverseAccessor {
    /// List the empty root to obtain all logical names, including folder paths,
    /// in API order. Names are opaque; both recursion settings return this list.
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        self.inner.list(request).await
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

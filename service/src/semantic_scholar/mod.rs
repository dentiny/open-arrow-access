//! Semantic Scholar release-pinned bulk datasets and incremental diff manifests.
//! Signed download URLs are preserved exactly.
use crate::common::{ApiDocument, ApiKey, Connection, RemoteFiles, segment, text};
mod error;
mod parser;
mod validation;
use open_arrow_access_core::Result;

#[derive(Clone, Debug)]
pub struct SemanticScholarService {
    connection: Connection,
}
#[derive(Clone, Debug)]
pub struct DatasetMetadata {
    pub release_id: String,
    pub release: ApiDocument,
    pub dataset: ApiDocument,
}
impl SemanticScholarService {
    pub fn new(api_key: &str) -> Result<Self> {
        Self::with_connection(Connection::new("https://api.semanticscholar.org")?).with_key(api_key)
    }

    /// Set or replace the API key while preserving the configured connection.
    /// The key is validated locally and sent as `x-api-key` only within its origin.
    pub fn with_key(mut self, key: &str) -> Result<Self> {
        self.connection = self.connection.with_api_key_message(
            ApiKey::Header("x-api-key"),
            key,
            "Semantic Scholar requires an API key",
        )?;
        Ok(self)
    }

    pub fn with_connection(connection: Connection) -> Self {
        Self { connection }
    }

    pub async fn release(&self, id: &str) -> Result<ApiDocument> {
        self.connection
            .json(&format!("datasets/v1/release/{}", segment(id)?), &[])
            .await?
            .validated(|value| validation::validate_release(value, id))
    }

    /// Resolve latest once, then obtain signed download URLs using the immutable release ID.
    pub async fn dataset(
        &self,
        release: &str,
        dataset: &str,
    ) -> Result<RemoteFiles<DatasetMetadata>> {
        let release = self.release(release).await?;
        let release_id = text(release.value(), "release_id")?.to_owned();
        let doc = self
            .connection
            .json(
                &format!(
                    "datasets/v1/release/{}/dataset/{}",
                    segment(&release_id)?,
                    segment(dataset)?
                ),
                &[],
            )
            .await?;
        let files = parser::parse_dataset_files(doc.value(), dataset)?;
        self.connection.files(
            files,
            DatasetMetadata {
                release_id,
                release,
                dataset: doc,
            },
        )
    }

    /// Return one incremental diff manifest, preserving update and deletion semantics.
    pub async fn diffs(&self, dataset: &str, start: &str, end: &str) -> Result<ApiDocument> {
        self.connection
            .json(
                &format!(
                    "datasets/v1/diffs/{}/to/{}/{}",
                    segment(start)?,
                    segment(end)?,
                    segment(dataset)?
                ),
                &[],
            )
            .await?
            .validated(validation::validate_diffs)
    }
}

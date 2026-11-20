//! Semantic Scholar release-pinned bulk datasets and incremental diff manifests.
//! Signed download URLs are preserved exactly.
use crate::common::{ApiDocument, ApiKey, Connection, RemoteFiles, segment, text};
mod cache;
pub use cache::{CachedSemanticScholarService, ManifestCacheOptions};
mod error;
mod parser;
mod v2;
mod validation;
use open_arrow_access_core::Result;
pub use v2::{S2orcV2Accessor, s2orc_v2_schema};

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

    /// Fetch one dataset manifest for a resolved, immutable release ID.
    /// Does not request the release document or download any dataset content.
    /// Resolve `latest` with [`Self::release`] first. Provider identity and
    /// partition URL string types are validated; binding files validates URLs.
    pub async fn dataset_manifest(&self, release_id: &str, dataset: &str) -> Result<ApiDocument> {
        if release_id.is_empty() || release_id == "latest" || dataset.is_empty() {
            return Err(crate::common::invalid(
                "dataset manifest requires a resolved release ID and dataset name",
            ));
        }
        let document = self
            .connection
            .json(
                &format!(
                    "datasets/v1/release/{}/dataset/{}",
                    segment(release_id)?,
                    segment(dataset)?
                ),
                &[],
            )
            .await?;
        parser::parse_dataset_files(document.value(), dataset)?;
        Ok(document)
    }

    /// Resolve latest once, then obtain signed download URLs using the immutable release ID.
    pub async fn dataset(
        &self,
        release: &str,
        dataset: &str,
    ) -> Result<RemoteFiles<DatasetMetadata>> {
        let release = self.release(release).await?;
        let release_id = text(release.value(), "release_id")?.to_owned();
        let doc = self.dataset_manifest(&release_id, dataset).await?;
        self.files_from_manifest(
            dataset,
            DatasetMetadata {
                release_id,
                release,
                dataset: doc,
            },
        )
    }

    /// Bind previously fetched metadata to lazy downloads without any API request.
    /// Revalidates the pinned release identity, dataset identity and signed URLs;
    /// URLs are retained byte-for-byte. The caller owns freshness and credential
    /// provenance of imported metadata; binding does not renew expired URLs.
    pub fn files_from_manifest(
        &self,
        dataset: &str,
        metadata: DatasetMetadata,
    ) -> Result<RemoteFiles<DatasetMetadata>> {
        if metadata.release_id.is_empty() || metadata.release_id == "latest" || dataset.is_empty() {
            return Err(crate::common::invalid(
                "file binding requires a resolved release ID and dataset name",
            ));
        }
        validation::validate_release(metadata.release.value(), &metadata.release_id)?;
        let files = parser::parse_dataset_files(metadata.dataset.value(), dataset)?;
        self.connection.files(files, metadata)
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

//! Semantic Scholar release-pinned bulk datasets and incremental diff manifests.
use crate::common::{ApiDocument, Connection, RemoteFiles, segment, text};
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
        validation::validate_key(api_key)?;
        Ok(Self::with_connection(
            Connection::new("https://api.semanticscholar.org")?
                .with_header("x-api-key", api_key)?,
        ))
    }

    pub fn with_connection(connection: Connection) -> Self {
        Self { connection }
    }

    pub async fn release(&self, id: &str) -> Result<ApiDocument> {
        let doc = self
            .connection
            .json(&format!("datasets/v1/release/{}", segment(id)?), &[])
            .await?;
        validation::validate_release(doc.value(), id)?;
        Ok(doc)
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
        let doc = self
            .connection
            .json(
                &format!(
                    "datasets/v1/diffs/{}/to/{}/{}",
                    segment(start)?,
                    segment(end)?,
                    segment(dataset)?
                ),
                &[],
            )
            .await?;
        validation::validate_diffs(doc.value())?;
        Ok(doc)
    }
}

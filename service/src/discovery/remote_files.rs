//! Bind discovered metadata links to lazy HTTP reads.
//! These inherent methods retain the existing Connection convenience API.
use super::{MetadataLink, ResolvedFileSet, file_manifest::FileSpec};
use crate::common::{ApiDocument, Connection, RemoteFiles, error};
use open_arrow_access_core::Result;

impl Connection {
    /// Resolve file links into lazy downloads, retaining the full JSON as metadata.
    /// The resolver owns provider identity/availability checks.
    pub async fn resolve_json_files(
        &self,
        path: &str,
        query: &[(&str, &str)],
        resolve: impl FnOnce(&ApiDocument) -> Result<Vec<MetadataLink>> + Send,
    ) -> Result<RemoteFiles> {
        let document = self.json(path, query).await?;
        let files = resolve(&document)?;
        self.files(files, document)
    }

    /// Bind a validated provider manifest to lazy byte reads. Logical names remain opaque.
    pub fn files<M>(&self, files: Vec<MetadataLink>, metadata: M) -> Result<RemoteFiles<M>> {
        validate_downloads(&files)?;
        ResolvedFileSet::from_files(
            self.clone(),
            files.into_iter().map(FileSpec::from),
            metadata,
        )
        .map_err(error::file_namespace)
    }
}

fn validate_downloads(files: &[MetadataLink]) -> Result<()> {
    for file in files {
        crate::common::resolve_download_url(&file.url)?;
    }
    Ok(())
}

//! Bind discovered metadata links to lazy HTTP reads.
//! These inherent methods retain the existing Connection convenience API.
use super::{MetadataLink, ResolvedFileSet, ResolvedMetadata, file_manifest::FileSpec};
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
        let metadata = self.resolve_json_metadata(path, query, resolve).await?;
        self.bind_metadata(metadata)
    }

    /// Resolve provider metadata and absolute file URLs without constructing storage.
    /// Provider validation runs here; download URL and namespace checks run at binding.
    pub async fn resolve_json_metadata(
        &self,
        path: &str,
        query: &[(&str, &str)],
        resolve: impl FnOnce(&ApiDocument) -> Result<Vec<MetadataLink>> + Send,
    ) -> Result<ResolvedMetadata<ApiDocument>> {
        let document = self.json(path, query).await?;
        let files = resolve(&document)?
            .into_iter()
            .map(FileSpec::from)
            .collect();
        Ok(ResolvedMetadata::from_parts(document, files, ()).with_origin(self.origin().clone()))
    }

    /// Bind a validated provider manifest to lazy byte reads. Logical names remain opaque.
    pub fn files<M>(&self, files: Vec<MetadataLink>, metadata: M) -> Result<RemoteFiles<M>> {
        self.bind_metadata(ResolvedMetadata::from_parts(
            metadata,
            files.into_iter().map(FileSpec::from).collect(),
            (),
        ))
    }

    /// Bind already resolved absolute URLs without making an API or download request.
    /// Authentication and HTTP policy come from this connection. Signed URLs are
    /// retained verbatim and are not refreshed by binding.
    pub fn bind_metadata<M>(&self, resolved: ResolvedMetadata<M>) -> Result<RemoteFiles<M>> {
        let (metadata, files, ()) = resolved.into_parts();
        for file in &files {
            crate::common::resolve_download_url(file.object.locator())?;
        }
        ResolvedFileSet::from_files(self.clone(), files, metadata).map_err(error::file_namespace)
    }
}

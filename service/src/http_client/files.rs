//! Bind a provider's validated manifest to lazy HTTP file access.

use open_arrow_access_core::{Error, ObjectRef, Result};
use open_arrow_access_storage::{NamedFileAccessor, OpenDalAccessor};
use serde::de::DeserializeOwned;
use url::Url;

use super::{HttpClient, validation::validate_origin};
use crate::ResolvedFileSet;

/// Canonical download root, ordered logical-name mappings and provider metadata.
/// Providers validate identity, availability and names before returning this plan.
pub(crate) struct ResolvedFilesPlan<M> {
    pub(crate) root: String,
    pub(crate) files: Vec<(String, ObjectRef)>,
    pub(crate) metadata: M,
}

/// Connection and error context shared by public JSON-manifest providers.
#[derive(Clone, Debug)]
pub(crate) struct HttpFileService {
    endpoint: Url,
    client: HttpClient,
    operation: &'static str,
    membership_error: fn(Error) -> Error,
}

impl HttpFileService {
    pub(crate) fn new(
        endpoint: &str,
        configure: &'static str,
        operation: &'static str,
        membership_error: fn(Error) -> Error,
    ) -> Result<Self> {
        Ok(Self {
            endpoint: validate_origin(endpoint, configure)?,
            client: HttpClient::new()?,
            operation,
            membership_error,
        })
    }

    pub(crate) fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    /// Fetch one public JSON manifest, validate/map it, and bind the fixed file
    /// namespace over the same transport. Resolving and listing fetch no file bodies;
    /// namespace errors use the provider's existing context and source chain.
    pub(crate) async fn resolve_files<T: DeserializeOwned, M>(
        &self,
        metadata_url: &Url,
        resolve: impl FnOnce(T) -> Result<ResolvedFilesPlan<M>>,
    ) -> Result<ResolvedFileSet<OpenDalAccessor, M>> {
        let response = self
            .client
            .json(metadata_url.as_str(), None, self.operation)
            .await?;
        let plan = resolve(response)?;
        let storage = self.client.accessor(&self.endpoint, &plan.root, &[])?;
        let files = NamedFileAccessor::new(storage, plan.files).map_err(self.membership_error)?;
        Ok(ResolvedFileSet::new(files, plan.metadata))
    }
}

#[cfg(test)]
#[path = "../../tests/unit/http_files.rs"]
mod tests;

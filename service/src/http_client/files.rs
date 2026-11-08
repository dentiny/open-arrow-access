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
    /// Download path prefix relative to the service's HTTP origin.
    root: String,
    /// Ordered (logical filename, download object) mappings. The filename is
    /// exposed by listing/open; the object's locator is relative to `root`.
    files: Vec<(String, ObjectRef)>,
    /// Provider metadata retained alongside the resolved file namespace.
    metadata: M,
    /// (Query parameter name, value) pairs appended to download requests and
    /// URL-encoded by the transport; metadata requests keep their own query.
    query: Vec<(String, String)>,
    /// (Download locator relative to `root`, byte length) pairs for the downloaded
    /// representation. Keys match `files` object locators, not logical filenames.
    /// Supplied lengths are trusted; missing locators fall back to storage stat.
    content_lengths: Vec<(String, u64)>,
}

impl<M> ResolvedFilesPlan<M> {
    pub(crate) fn new(
        root: impl Into<String>,
        files: Vec<(String, ObjectRef)>,
        metadata: M,
    ) -> Self {
        Self {
            root: root.into(),
            files,
            metadata,
            query: Vec::new(),
            content_lengths: Vec::new(),
        }
    }

    /// Query parameters apply to file downloads, not the metadata request.
    pub(crate) fn with_query(mut self, query: &[(&str, &str)]) -> Self {
        self.query = query
            .iter()
            .map(|(key, value)| ((*key).into(), (*value).into()))
            .collect();
        self
    }

    /// Lengths are keyed by download locator and must describe that representation.
    /// Storage uses stat for locators without a supplied length.
    pub(crate) fn with_content_lengths(
        mut self,
        lengths: impl IntoIterator<Item = (String, u64)>,
    ) -> Self {
        self.content_lengths = lengths.into_iter().collect();
        self
    }
}

/// Connection and error context shared by public JSON-manifest providers.
#[derive(Clone, Debug)]
pub(crate) struct HttpFileService {
    endpoint: Url,
    client: HttpClient,
    /// Provider resolve operation (e.g. `resolve_zenodo`) for metadata HTTP/JSON errors.
    operation: &'static str,
    /// Maps empty/duplicate filenames to provider-specific decode errors,
    /// retaining the underlying namespace error as the source.
    map_file_namespace_error: fn(Error) -> Error,
}

impl HttpFileService {
    pub(crate) fn new(
        endpoint: &str,
        configure: &'static str,
        operation: &'static str,
        map_file_namespace_error: fn(Error) -> Error,
    ) -> Result<Self> {
        Ok(Self {
            endpoint: validate_origin(endpoint, configure)?,
            client: HttpClient::new()?,
            operation,
            map_file_namespace_error,
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
            .json(metadata_url.as_str(), /*token=*/ None, self.operation)
            .await?;
        let plan = resolve(response)?;
        let storage = self
            .client
            .accessor(&self.endpoint, &plan.root, plan.query)?
            .with_content_lengths(plan.content_lengths);
        let files =
            NamedFileAccessor::new(storage, plan.files).map_err(self.map_file_namespace_error)?;
        Ok(ResolvedFileSet::new(files, plan.metadata))
    }
}

//! Bind a provider's validated manifest to lazy HTTP file access.

use indexmap::IndexMap;
use open_arrow_access_core::{Error, ErrorKind, ObjectRef, Result};
use open_arrow_access_storage::{NamedFileAccessor, OpenDalAccessor};
use serde::de::DeserializeOwned;
use url::Url;

use super::{HttpClient, validation::validate_origin};
use crate::ResolvedFileSet;

/// One download representation, addressed relative to the plan's `root`.
pub(crate) struct ResolvedFile {
    pub(crate) object: ObjectRef,
    /// Trusted byte length of this representation; None falls back to storage stat.
    pub(crate) content_length: Option<u64>,
}

/// Canonical download root, ordered logical-name mappings and provider metadata.
/// Providers validate identity, availability and names before returning this plan.
pub(crate) struct ResolvedFilesPlan<M> {
    /// Download path prefix relative to the service's HTTP origin.
    root: String,
    /// Logical filename -> download object and optional trusted byte length.
    /// Insertion order is manifest/listing order; empty and duplicate names fail.
    files: IndexMap<String, ResolvedFile>,
    /// Provider metadata retained alongside the resolved file namespace.
    metadata: M,
    /// (Query parameter name, value) pairs appended to download requests and
    /// URL-encoded by the transport; metadata requests keep their own query.
    query: Vec<(String, String)>,
}

impl<M> ResolvedFilesPlan<M> {
    pub(crate) fn new(
        root: impl Into<String>,
        files: Vec<(String, ResolvedFile)>,
        metadata: M,
    ) -> Result<Self> {
        let mut ordered = IndexMap::with_capacity(files.len());
        for (name, file) in files {
            if name.is_empty() || ordered.insert(name, file).is_some() {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "configure_resolved_files",
                    "empty or duplicate filename",
                ));
            }
        }
        Ok(Self {
            root: root.into(),
            files: ordered,
            metadata,
            query: Vec::new(),
        })
    }

    /// Query parameters apply to file downloads, not the metadata request.
    pub(crate) fn with_query(mut self, query: &[(&str, &str)]) -> Self {
        self.query = query
            .iter()
            .map(|(key, value)| ((*key).into(), (*value).into()))
            .collect();
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
        let lengths = plan.files.values().filter_map(|file| {
            file.content_length
                .map(|length| (file.object.locator().to_owned(), length))
        });
        let storage = self
            .client
            .accessor(&self.endpoint, &plan.root, plan.query)?
            .with_content_lengths(lengths);
        let files = NamedFileAccessor::new(
            storage,
            plan.files
                .into_iter()
                .map(|(name, file)| (name, file.object))
                .collect(),
        )
        .map_err(self.map_file_namespace_error)?;
        Ok(ResolvedFileSet::new(files, plan.metadata))
    }
}

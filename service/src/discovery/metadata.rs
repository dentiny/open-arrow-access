//! Storage-independent results shared by URL-backed and rooted-file providers.
use super::ResolvedFile;
use url::Url;

/// Provider metadata and ordered file descriptions before storage is constructed.
///
/// The default location `()` means file locators are absolute HTTP URLs. A
/// [`HttpFileRoot`] describes locators relative to a provider download root.
/// Identity checks are provider-owned; URL/namespace checks remain at binding.
/// This value contains no reader, operator, transport or credentials.
#[derive(Clone, Debug)]
pub struct ResolvedMetadata<M, L = ()> {
    metadata: M,
    files: Vec<ResolvedFile>,
    location: L,
    origin: Option<Url>,
}

impl<M, L> ResolvedMetadata<M, L> {
    pub(crate) fn from_parts(metadata: M, files: Vec<ResolvedFile>, location: L) -> Self {
        Self {
            metadata,
            files,
            location,
            origin: None,
        }
    }

    pub(crate) fn with_origin(mut self, origin: Url) -> Self {
        self.origin = Some(origin);
        self
    }

    /// HTTP endpoint at which these resources were resolved, when available.
    pub fn origin(&self) -> Option<&Url> {
        self.origin.as_ref()
    }

    pub fn metadata(&self) -> &M {
        &self.metadata
    }

    /// Exact provider order and aliases; no listing or download requests are made.
    pub fn files(&self) -> &[ResolvedFile] {
        &self.files
    }

    pub fn location(&self) -> &L {
        &self.location
    }

    pub fn into_metadata(self) -> M {
        self.metadata
    }

    pub(crate) fn into_parts(self) -> (M, Vec<ResolvedFile>, L) {
        (self.metadata, self.files, self.location)
    }
}

/// Download root and query interpreted at the resolving service's HTTP endpoint.
/// This layout contains no authentication or transport configuration.
#[derive(Clone, Debug)]
pub struct HttpFileRoot {
    pub(crate) root: String,
    pub(crate) query: Vec<(String, String)>,
}

impl HttpFileRoot {
    pub fn root(&self) -> &str {
        &self.root
    }

    pub fn query(&self) -> &[(String, String)] {
        &self.query
    }
}

impl<M> ResolvedMetadata<M, HttpFileRoot> {
    pub(crate) fn new(root: impl Into<String>, files: Vec<ResolvedFile>, metadata: M) -> Self {
        Self::from_parts(
            metadata,
            files,
            HttpFileRoot {
                root: root.into(),
                query: Vec::new(),
            },
        )
    }

    pub(crate) fn with_query(mut self, query: &[(&str, &str)]) -> Self {
        self.location.query = query
            .iter()
            .map(|(key, value)| ((*key).into(), (*value).into()))
            .collect();
        self
    }
}

use open_arrow_access_core::{
    ByteReadOptions, ObjectRef, ReadOptions, ReadOptionsProvider, Result,
};

/// A metadata prefix or explicit subset in a bucket-root PMC mirror.
///
/// Prefix discovery is recursive and streaming, in storage listing order.
/// This is a live listing, not an atomic release snapshot. For reproducibility,
/// supply a versioned mirror prefix or an explicit list of metadata objects.
#[derive(Clone, Debug)]
pub struct PmcRequest {
    pub(super) selection: Selection,
    pub options: ReadOptions,
    pub byte_options: ByteReadOptions,
}

#[derive(Clone, Debug)]
pub(super) enum Selection {
    Prefix(String),
    Files(Vec<ObjectRef>),
}

impl PmcRequest {
    /// Read all article-version metadata under the public `metadata/` prefix.
    pub fn release() -> Self {
        Self::prefix("metadata/")
    }

    /// Recursively discover lowercase `.json` metadata under a bucket-relative prefix.
    /// Include a trailing slash and choose a metadata-only prefix; unrelated JSON
    /// documents would also be treated as article metadata.
    pub fn prefix(prefix: impl Into<String>) -> Self {
        Self {
            selection: Selection::Prefix(prefix.into()),
            options: ReadOptions::default(),
            byte_options: ByteReadOptions::default(),
        }
    }

    /// Read exact bucket-relative `.json` objects without listing.
    /// Caller order and duplicates are retained; an empty set yields no rows.
    pub fn files(objects: impl IntoIterator<Item = impl Into<ObjectRef>>) -> Self {
        Self {
            selection: Selection::Files(objects.into_iter().map(Into::into).collect()),
            options: ReadOptions::default(),
            byte_options: ByteReadOptions::default(),
        }
    }

    /// Interpret `pmc://oa`, `pmc://oa/<prefix>/`, or `pmc://oa/<object>.json`.
    /// The URI selects paths only; storage remains controlled by the accessor.
    pub fn from_uri(uri: &str) -> Result<Self> {
        let suffix = uri
            .strip_prefix("pmc://oa")
            .ok_or_else(super::error::invalid_selection)?;
        if suffix.is_empty() || suffix == "/" {
            return Ok(Self::release());
        }
        let path = suffix
            .strip_prefix('/')
            .ok_or_else(super::error::invalid_selection)?;
        super::validation::validate_metadata_path(path, path.ends_with('/'))?;
        Ok(if path.ends_with('/') {
            Self::prefix(path)
        } else {
            Self::files([path])
        })
    }

    pub fn with_options(mut self, options: ReadOptions) -> Self {
        self.options = options;
        self
    }

    pub fn with_byte_options(mut self, options: ByteReadOptions) -> Self {
        self.byte_options = options;
        self
    }

    pub(super) fn validate(&self) -> Result<()> {
        match &self.selection {
            Selection::Prefix(prefix) => super::validation::validate_metadata_path(prefix, true),
            Selection::Files(files) => {
                for file in files {
                    super::validation::validate_metadata_path(file.locator(), false)?;
                }
                Ok(())
            }
        }
    }
}

impl ReadOptionsProvider for PmcRequest {
    fn read_options(&self) -> &ReadOptions {
        &self.options
    }
    fn read_options_mut(&mut self) -> &mut ReadOptions {
        &mut self.options
    }
}

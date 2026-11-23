//! Shared file descriptions; locators are interpreted only by the bound storage.
use open_arrow_access_core::ObjectRef;

/// One logical name and its download representation. Aliases may share an object.
/// Lengths describe the representation, never the logical name or decoded content.
#[derive(Clone, Debug)]
pub struct ResolvedFile {
    pub(crate) name: String,
    pub(crate) object: ObjectRef,
    pub(crate) content_length: Option<u64>,
}

impl From<crate::common::MetadataLink> for ResolvedFile {
    fn from(link: crate::common::MetadataLink) -> Self {
        Self {
            name: link.file_name,
            object: link.url.into(),
            content_length: None,
        }
    }
}

impl ResolvedFile {
    /// Exact logical name advertised by the provider.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Locator interpreted by the resolved metadata's location and storage binding.
    pub fn object(&self) -> &ObjectRef {
        &self.object
    }

    /// Known length of the download representation, without performing storage I/O.
    pub fn content_length(&self) -> Option<u64> {
        self.content_length
    }
}

// Preserve the existing internal construction name for provider manifests.
pub(crate) type FileSpec = ResolvedFile;

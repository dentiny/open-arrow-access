//! Shared file descriptions; locators are interpreted only by the bound storage.
use open_arrow_access_core::ObjectRef;

/// One logical name and its download representation. Aliases may share an object.
/// Lengths describe the representation, never the logical name or decoded content.
pub(crate) struct FileSpec {
    pub(crate) name: String,
    pub(crate) object: ObjectRef,
    pub(crate) content_length: Option<u64>,
    /// Provider-reported download size for manifest rows; not a range-read policy.
    pub(crate) size: Option<u64>,
}

impl From<crate::common::MetadataLink> for FileSpec {
    fn from(link: crate::common::MetadataLink) -> Self {
        Self {
            name: link.file_name,
            object: link.url.into(),
            content_length: None,
            size: None,
        }
    }
}

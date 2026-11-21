//! One selected attachment value, before Arrow row/batch assembly.
use std::sync::Arc;

use crate::{Bytes, ObjectRef, Result, async_trait};

/// Content representation, independent of the Arrow type used to store its bytes.
/// Persist [`Self::as_str`] as a string/dictionary tag beside binary content.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AttachmentDataType {
    Txt,
    Latex,
    Pdf,
    Xml,
    Html,
    Binary,
    /// Provider-defined representation, without changing the fallback layer.
    Other(String),
}

impl AttachmentDataType {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Txt => "txt",
            Self::Latex => "latex",
            Self::Pdf => "pdf",
            Self::Xml => "xml",
            Self::Html => "html",
            Self::Binary => "binary",
            Self::Other(value) => value,
        }
    }
}

/// One complete attachment cell. Format and provenance travel with the payload.
/// An empty payload is an existing value; absence is represented by `None`.
/// Providers assemble cells into Arrow rows/batches outside this interface.
///
/// ```
/// use open_arrow_access_core::{AttachmentCell, AttachmentDataType, Bytes};
/// let cell = AttachmentCell::new(AttachmentDataType::Pdf, Bytes::from_static(b"%PDF-1.7"));
/// assert_eq!(cell.data_type.as_str(), "pdf");
/// assert_eq!(cell.data.as_ref(), b"%PDF-1.7");
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttachmentCell {
    pub data_type: AttachmentDataType,
    pub data: Bytes,
    pub source: Option<ObjectRef>,
}

impl AttachmentCell {
    pub fn new(data_type: AttachmentDataType, data: impl Into<Bytes>) -> Self {
        Self {
            data_type,
            data: data.into(),
            source: None,
        }
    }

    pub fn with_source(mut self, source: impl Into<ObjectRef>) -> Self {
        self.source = Some(source.into());
        self
    }
}

/// Read a single attachment cell for one logical item, without a record stream.
/// Providers own format selection, identity/version resolution and storage I/O.
#[async_trait]
pub trait AttachmentCellAccessor: Send + Sync {
    type Request: Send;

    /// Return `None` only when this representation is absent. Successful empty
    /// content is `Some(cell)`. Transport, authentication and decoding failures
    /// are errors, not missing values. Cancel by dropping the read future.
    async fn read_cell(&self, request: Self::Request) -> Result<Option<AttachmentCell>>;
}

#[async_trait]
impl<A: AttachmentCellAccessor + ?Sized> AttachmentCellAccessor for Arc<A> {
    type Request = A::Request;

    async fn read_cell(&self, request: Self::Request) -> Result<Option<AttachmentCell>> {
        (**self).read_cell(request).await
    }
}

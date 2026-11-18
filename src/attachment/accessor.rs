use super::{AttachmentOptions, open_attachment_source};
use crate::{
    Accessor, ByteAccessor, OrderRequirement, ReadRequest, RecordCapability, RecordReader, Result,
    async_trait,
};
use std::sync::Arc;

/// Reusable record accessor for explicitly selected provider attachments.
///
/// The byte accessor retains membership, credentials, and transport policy. Each
/// [`Accessor::open`] independently decodes the request's logical name with the
/// same format options, preserving its projection, batch size, and byte options.
/// Constructing or cloning this adapter performs no I/O; it shares the byte
/// accessor through an [`Arc`] without requiring the accessor itself to be `Clone`.
///
/// Use [`open_attachment_source`] to bind one request immediately, or pass this
/// adapter to [`crate::BoundSource::bind`] or [`crate::PreparedSource::prepare`]
/// when the consumer needs an accessor rather than a bound source.
pub struct AttachmentAccessor<A> {
    accessor: Arc<A>,
    options: AttachmentOptions,
}

impl<A: ByteAccessor + 'static> AttachmentAccessor<A> {
    /// Validate decoder configuration without accessing provider storage.
    ///
    /// # Errors
    /// Returns the same configuration and disabled-format errors as
    /// [`AttachmentOptions::validate`]. Request-specific errors are returned
    /// when opening a reader.
    pub fn new(accessor: A, options: AttachmentOptions) -> Result<Self> {
        options.validate()?;
        Ok(Self {
            accessor: Arc::new(accessor),
            options,
        })
    }
}

impl<A> Clone for AttachmentAccessor<A> {
    fn clone(&self) -> Self {
        Self {
            accessor: self.accessor.clone(),
            options: self.options.clone(),
        }
    }
}

#[async_trait]
impl<A: ByteAccessor + 'static> Accessor for AttachmentAccessor<A> {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        // A provider's byte-range or listing support does not imply record
        // partition planning. Each returned decoder keeps its input ordering.
        RecordCapability::sequential(OrderRequirement::PreserveGlobal)
    }

    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        open_attachment_source(self.accessor.clone(), request, self.options.clone())
            .await?
            .reader()
            .await
    }
}
